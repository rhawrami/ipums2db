// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import (
	"errors"
	"fmt"
	"io"
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

// the maximum value for a 32 bit signed int is (2 ** 31 - 1)
// or 2147483647. This value has ten places. So we need to limit
// INT columns to those with widths <= 10.
const maxPlacesFori32 int = 10

// getDataTypes returns a map of traditional types and their
// database system-specific equivalents
//
// returns error if dbType is not one of the supported and recognized types
func getDataTypes(dbType string) (map[string]string, error) {
	types2DBtypes := map[string]string{
		"int":    "INT",
		"float":  "NUMERIC",
		"string": "VARCHAR",
	}

	switch strings.ToLower(dbType) {
	case POSTGRES, MSSQL:
	case MYSQL:
		types2DBtypes["float"] = "DECIMAL"
	case ORACLE:
		types2DBtypes["float"] = "NUMBER"
		types2DBtypes["string"] = "VARCHAR2"
	default:
		return nil, fmt.Errorf("unrecognized database type '%s' not in {'postgres', 'oracle', 'mysql', mssql'}", dbType)
	}

	return types2DBtypes, nil
}

// NewDBFormatter returns a pointer to a DatabaseFormatter,
// taking the database system, and main table name, as inputs
//
// returns error if unrecognized/unsupported database system
func NewDBFormatter(dbType, tableName string) (*DatabaseFormatter, error) {
	if len(tableName) == 0 {
		return nil, fmt.Errorf("tableName can not be empty")
	}
	dataTypes, err := getDataTypes(dbType)
	if err != nil {
		return nil, fmt.Errorf("could not get data types: %w", err)
	}

	return &DatabaseFormatter{DbType: dbType, TableName: tableName, DataTypes: dataTypes}, nil
}

// DatabaseFormatter contains a relational database system identifier and
// a corresponding map of traditional and database types
type DatabaseFormatter struct {
	DbType    string
	TableName string
	DataTypes map[string]string
}

// CreateMainTable generates a SQL "CREATE TABLE" statement, given a data dictionary and table name,
// returning a byte slice of the creation statement (note: statement terminator (e.g., ";") is included)
//
// returns error if a variable's interval type is not in {"contin", "discrete"}
func (dbf *DatabaseFormatter) CreateMainTable(ddi *DataDict) ([]byte, error) {
	init_statement := fmt.Sprintf("CREATE TABLE %s (", dbf.TableName)
	ddl_table := init_statement

	for i, v := range ddi.Vars {
		var typeToUse, nameAndType string
		// if a var has decimal places, make it float
		if v.DecimalPoint != 0 {
			// make numeric type with precision := width; scale := decimalpoint
			typeToUse = fmt.Sprintf("%s(%d,%d)", dbf.DataTypes["float"], v.Location.Width, v.DecimalPoint)
		} else {
			switch v.Interval {
			case "contin", "discrete":
				// the INT type in these database systems defaults to 32 bits
				// since there's no negative value in these extracts, you can represent
				// integers from 0 to 2^32 - 1 (which has ten places)
				// if for some reason, a column has more than 10 characters,
				// make it a string type
				if v.Location.Width > maxPlacesFori32 {
					typeToUse = fmt.Sprintf("%s(%d)", dbf.DataTypes["string"], v.Location.Width) // make varchar(N) for var with max N chars
				} else {
					typeToUse = dbf.DataTypes["int"] // the rest of vars are ints
				}
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
		nameAndType = fmt.Sprintf("\n\t%s %s%s\t-- %s", strings.ToLower(v.Name), typeToUse, addComma, v.Label)
		ddl_table += nameAndType
	}
	ddl_table += "\n);\n\n"

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

			catAndType := fmt.Sprintf("\n\tval %s,\n\tlabel %s\n);\n\n", dbf.DataTypes["int"], dbf.DataTypes["string"])
			refTable += catAndType
			ddlStatement += refTable

			insertStatement := fmt.Sprintf("INSERT INTO %s (val, label)\nVALUES", tableName)
			for i, cat := range v.Cats {
				var addComma string
				if i == (len(v.Cats) - 1) {
					addComma = "\n"
				} else {
					addComma = ","
				}
				escapedLabel := strings.ReplaceAll(cat.Label, "'", "''")
				valAndLab := fmt.Sprintf("\n\t(%s, '%s')%s", cat.Val, escapedLabel, addComma)
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

// CreateIndices generates "CREATE INDEX idx_var" statements for a set of columns. As of now, does not
// support multi-column index creations.
//
// returns error if a column is not recognized in the data dictionary
func (dbf *DatabaseFormatter) CreateIndices(ddi *DataDict, cols []string) ([]byte, error) {
	indexStatements := ""
	varNames := dbf.VariableNames(ddi)
	for _, col := range cols {
		if !slices.Contains(varNames, strings.ToLower(col)) {
			return nil, fmt.Errorf("cannot create idx on unrecognized variable %s", col)
		}
		indexStatements += fmt.Sprintf("CREATE INDEX idx_%s ON %s (%s);\n\n", col, dbf.TableName, col)
	}
	return []byte(indexStatements), nil
}

// VariableNames returns the included variables from a data dictionary
func (dbf *DatabaseFormatter) VariableNames(ddi *DataDict) []string {
	variableNames := make([]string, len(ddi.Vars))
	for i, v := range ddi.Vars {
		variableNames[i] = strings.ToLower(v.Name)
	}
	return variableNames
}

// BulkInsert generates mulit-tuple database table inserts.
//
// It takes in a DataDict pointer, the fixed width file, the row
// in the file to start reading at, and the number of rows to parse in total.
//
// Returns error file can't be opened, or if any row cannot be parsed.
func (dbf *DatabaseFormatter) BulkInsert(ddi *DataDict, datFile *os.File, startAtRow int, numRows int) ([]byte, error) {
	bytesPerLine := BytesPerRow(ddi)

	off := bytesPerLine * startAtRow
	buffSize := numRows * bytesPerLine
	buffer := make([]byte, buffSize)
	_, err := datFile.ReadAt(buffer, int64(off))
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("error reading dat file: %v", err)
		}
	}

	bulkInsertInit := fmt.Sprintf("INSERT INTO %s VALUES\n", dbf.TableName)
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
			return nil, fmt.Errorf("startAt %d & endAt %d not valid index range for sliceLen %d", start, end, len(row))
		}

		chars := row[start:end]
		var sChars string
		// handle decimal places
		if v.DecimalPoint != 0 {
			placeDecimalAt := len(chars) - v.DecimalPoint
			chars = slices.Insert(chars, placeDecimalAt, byte('.'))
			sChars = string(chars)
		} else {
			if v.Location.Width > maxPlacesFori32 {
				sChars = fmt.Sprintf("'%s'", string(chars)) // handle string types
			} else {
				sChars = string(chars) // int types
			}
		}

		if i != (len(ddi.Vars) - 1) {
			insertStatement += sChars + ","
		} else {
			insertStatement += sChars
		}
	}
	insertStatement += "),\n"
	return []byte(insertStatement), nil
}
