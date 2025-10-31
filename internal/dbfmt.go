// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import (
	"fmt"
	"os"
	"slices"
	"strings"
)

// As of this initial version, the four following relational
// database systems will be supported
const (
	POSTGRES string = "postgres"
	ORACLE   string = "oracle"
	MYSQL    string = "mysql"
	MSSQL    string = "mssql"
)

// getDataTypes returns a map of traditional types and their
// database system-specific equivalents
//
// returns error if dbType is not one of the supported and recognized types
func getDataTypes(dbType string) (map[string]string, error) {
	types2DBtypes := map[string]string{
		"int":    "INT",
		"float":  "NUMERIC",
		"string": "TEXT",
	}

	switch strings.ToLower(dbType) {
	case POSTGRES, MYSQL, MSSQL:
		return types2DBtypes, nil
	case ORACLE:
		types2DBtypes["float"] = "NUMBER"
		types2DBtypes["string"] = "varchar2(4000)"
		return types2DBtypes, nil
	default:
		return nil, fmt.Errorf("unrecognized database type %s not in {'postgres', 'oracle', 'mysql', mssql'}", dbType)
	}
}

// NewDBFormatter returns a pointer to a DatabaseFormatter,
// taking the database system as an input
//
// returns error if unrecognized/unsupported database system
func NewDBFormatter(dbType string) (*DatabaseFormatter, error) {
	dataTypes, err := getDataTypes(dbType)
	if err != nil {
		return nil, fmt.Errorf("could not get data types: %w", err)
	}

	return &DatabaseFormatter{DbType: dbType, DataTypes: dataTypes}, nil
}

// DatabaseFormatter contains a relational database system identifier and
// a corresponding map of traditional and database types
type DatabaseFormatter struct {
	DbType    string
	DataTypes map[string]string
}

// CreateMainTable generates a SQL "CREATE TABLE" statement, given a data dictionary and table name,
// returning a byte slice of the creation statement (note: statement terminator (e.g., ";") is included)
//
// returns error if a variable's interval type is not in {"contin", "discrete"}
func (dbf *DatabaseFormatter) CreateMainTable(ddi *DataDict, tableName string) ([]byte, error) {
	init_statement := fmt.Sprintf("CREATE TABLE %s (", tableName)
	ddl_table := init_statement

	for i, v := range ddi.Vars {
		var typeToUse, nameAndType string
		if v.DecimalPoint != 0 {
			typeToUse = dbf.DataTypes["float"]
		} else {
			switch v.Interval {
			case "contin", "discrete":
				typeToUse = dbf.DataTypes["int"]
			default:
				return nil, fmt.Errorf("unrecognized interval type %s for var %s", strings.ToLower(v.Name), v.Interval)
			}
		}
		var addComma string
		if i == (len(ddi.Vars) - 1) {
			addComma = ""
		} else {
			addComma = ","
		}
		nameAndType = fmt.Sprintf("\n\t%v %v%v\t-- %v", strings.ToLower(v.Name), typeToUse, addComma, v.Label)
		ddl_table += nameAndType
	}
	ddl_table += "\n);\n"

	return []byte(ddl_table), nil
}

// CreateRefTables generates "CREATE TABLE" and "INSERT INTO ref_var" statements for the set of discrete variables in a data-dictionary, returning
// a byte slice of all the statements (note: statement terminator (e.g., ";") is included).
//
// For example, the variable LABFORCE would generate the statements:
//
// CREATE TABLE ref_labforce (
//
//	val INT,
//	label TEXT);
//
// );
//
// INSERT INTO ref_labforce (val, label)
// VALUES
//
//	(0, 'N/A'),
//	(1, 'No, not in the labor force'),
//	(2, 'Yes, in the labor force'),
//	(9, 'Unclassifiable (employment status unknown)');
//
// returns error if data dictionary contains zero discrete variables
func (dbf *DatabaseFormatter) CreateRefTables(ddi *DataDict) ([]byte, error) {
	ddlStatement := ""
	discreteVarCtr := 0 // return err if no discrete variables (e.g., no table statements)
	for _, v := range ddi.Vars {
		if v.Interval == "discrete" {
			discreteVarCtr += 1
			tableName := "ref_" + strings.ToLower(v.Name)
			init_statement := fmt.Sprintf("CREATE TABLE %s (", tableName)
			refTable := init_statement

			catAndType := fmt.Sprintf("\n\tval %v,\n\tlabel %v\n);\n\n", dbf.DataTypes["int"], dbf.DataTypes["string"])
			refTable += catAndType
			ddlStatement += refTable

			insertStatement := fmt.Sprintf("INSERT INTO %v (val, label)\nVALUES", tableName)
			for i, cat := range v.Cats {
				var addComma string
				if i == (len(v.Cats) - 1) {
					addComma = "\n"
				} else {
					addComma = ","
				}
				escapedLabel := strings.ReplaceAll(cat.Label, "'", "''")
				valAndLab := fmt.Sprintf("\n\t(%v, '%v')%v", cat.Val, escapedLabel, addComma)
				insertStatement += valAndLab
			}
			insertStatement += ";\n\n"
			ddlStatement += insertStatement
		}
	}
	if discreteVarCtr == 0 {
		return nil, fmt.Errorf("zero discrete variables included")
	}
	return []byte(ddlStatement), nil
}

func (dbf *DatabaseFormatter) ByteBulkInsert(ddi *DataDict, fileName string, startAtRow int, numRows int, tabName string) ([]byte, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bytesPerLine, err := BytesPerRow(ddi)
	if err != nil {
		return nil, err
	}

	off := bytesPerLine * startAtRow
	buffSize := numRows * bytesPerLine
	buffer := make([]byte, buffSize)
	_, _ = file.ReadAt(buffer, int64(off))

	bulkInsertInit := fmt.Sprintf("INSERT INTO %v VALUES\n", tabName)
	dat := make([]byte, 0, len(buffer))
	for i := 0; i < len(buffer); i += bytesPerLine {
		row := buffer[i:(i + bytesPerLine)]
		inserts, err := dbf.insertTuple(ddi, row)
		if err != nil {
			return nil, fmt.Errorf("error row %v: %w", row, err)
		}
		dat = append(dat, inserts...)
	}
	bulkInsertStatement := append([]byte(bulkInsertInit), dat...)
	bulkInsertStatement[len(bulkInsertStatement)-2] = ';'
	return bulkInsertStatement, nil
}

// BulkInsert generates a bulk insert statement for a slice of rows.
//
// returns error if any one row contains a parsing error.
func (dbf *DatabaseFormatter) BulkInsert(ddi *DataDict, rows [][]byte, tabName string) ([]byte, error) {
	bulkInsertInit := fmt.Sprintf("INSERT INTO %v VALUES\n", tabName)
	dat := make([]byte, 0)
	for _, row := range rows {
		inserts, err := dbf.insertTuple(ddi, row)
		// come back to this;
		// currently, returning error for entire bulk insert, if just one row errs
		// if a single row is an issue for some reason, maybe skip?
		if err != nil {
			return nil, fmt.Errorf("error row %v: %w", row, err)
		}
		dat = append(dat, inserts...)

	}
	bulkInsertStatement := append([]byte(bulkInsertInit), dat...)
	bulkInsertStatement[len(bulkInsertStatement)-2] = ';'
	return bulkInsertStatement, nil
}

// insertTuple generates a single insertion tuple, given a row byte slice and a data dictionary.
// Note that this statement does not include the insertion statement itself, as the BulkInsert method
// will be used to create insertion statements.
//
// returns error if start and end positions are not valid for row.
func (dbf *DatabaseFormatter) insertTuple(ddi *DataDict, row []byte) ([]byte, error) {
	insertStatement := "\t("
	for i, v := range ddi.Vars {
		start, end := v.Location.Start-1, v.Location.End
		if (start < 0) || (end > len(row)) {
			return nil, fmt.Errorf("startAt %v & endAt %v not valid index range for sliceLen %v", start, end, len(row))
		}
		chars := row[start:end]
		if v.DecimalPoint != 0 {
			placeDecimalAt := len(chars) - v.DecimalPoint
			chars = slices.Insert(chars, placeDecimalAt, byte('.'))
		}
		if i != (len(ddi.Vars) - 1) {
			insertStatement += string(chars) + ","
		} else {
			insertStatement += string(chars)
		}
	}
	insertStatement += "),\n"
	return []byte(insertStatement), nil
}

// CreateIndices generates "CREATE INDEX idx_var" statements for a set of columns. As of now, does not
// support multi-column index creations.
//
// returns error if a column is not recognized in the data dictionary
func (dbf *DatabaseFormatter) CreateIndices(ddi *DataDict, cols []string, tableName string) ([]byte, error) {
	indexStatements := ""
	varNames := dbf.VariableNames(ddi)
	for _, col := range cols {
		if !slices.Contains(varNames, col) {
			return nil, fmt.Errorf("cannot create idx on unrecognized variable %v", col)
		}
		indexStatements += fmt.Sprintf("CREATE INDEX idx_%v ON %v (%v);\n", col, tableName, col)
	}
	return []byte(indexStatements), nil
}

// VariableNames returns the included variables from a data dictionary
func (dbf *DatabaseFormatter) VariableNames(ddi *DataDict) []string {
	variableNames := make([]string, len(ddi.Vars))
	for i, v := range ddi.Vars {
		variableNames[i] = v.Name
	}
	return variableNames
}
