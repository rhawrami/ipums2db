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

// the INT type in these database systems defaults to 32 bits
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
		"int":    "int",
		"float":  "numeric",
		"string": "varchar",
	}

	switch strings.ToLower(dbType) {
	case POSTGRES, MSSQL:
	case MYSQL:
		types2DBtypes["float"] = "decimal"
	case ORACLE:
		types2DBtypes["float"] = "number"
		types2DBtypes["string"] = "varchar2"
	default:
		return nil, fmt.Errorf("dbType '%s' not in {'postgres', 'oracle', 'mysql', mssql'}", dbType)
	}

	return types2DBtypes, nil
}

// NewDBFormatter returns a pointer to a DatabaseFormatter,
// taking the database system, and main table name, and mkddl as inputs
//
// returns error if unrecognized/unsupported database system
func NewDBFormatter(dbType, tableName string, mkddl bool) (*DatabaseFormatter, error) {
	if len(tableName) == 0 {
		return nil, fmt.Errorf("tableName can not be empty")
	}
	dataTypes, err := getDataTypes(dbType)
	if err != nil {
		return nil, fmt.Errorf("could not get data types: %w", err)
	}

	return &DatabaseFormatter{
		DbType:    dbType,
		TableName: tableName,
		DataTypes: dataTypes,
		mkddl:     mkddl,
	}, nil
}

// DatabaseFormatter contains a relational database system identifier and
// a corresponding map of traditional and database types
type DatabaseFormatter struct {
	DbType    string
	TableName string
	DataTypes map[string]string
	mkddl     bool
}

// CreateMainTable generates a SQL "CREATE TABLE" statement, given a data dictionary and table name,
// returning a byte slice of the creation statement (note: statement terminator (e.g., ";") is included)
//
// returns error if a variable's interval type is not in {"contin", "discrete"}
func (dbf *DatabaseFormatter) CreateMainTable(ddi *DataDict) ([]byte, error) {
	init_statement := fmt.Sprintf("CREATE TABLE %s (", dbf.TableName)
	var ddl_table strings.Builder
	ddl_table.WriteString(init_statement)

	// occasionally, you'll have column names like "where" or "year", which may
	// conflict with reserved keywords. So we need to "escape" the column names
	// in out table creation. The accepted characters for escaping are a little
	// different by system.
	var colEscChr string
	switch dbf.DbType {
	case "postgres", "oracle", "mssql":
		colEscChr = `"`
	case "mysql":
		colEscChr = "`"
	default:
	}

	for i, v := range ddi.Vars {
		var typeToUse, nameAndType strings.Builder
		// get column type
		switch colType := dbf.columnType(v); colType {
		case "float":
			typeToUse.WriteString(fmt.Sprintf("%s(%d,%d)", dbf.DataTypes["float"], v.Location.Width, v.DecimalPoint))
		case "string":
			typeToUse.WriteString(fmt.Sprintf("%s(%d)", dbf.DataTypes["string"], v.Location.Width))
		case "int":
			typeToUse.WriteString(dbf.DataTypes["int"]) // the rest of vars are ints
		default: // in future, maybe add other types
		}

		var addComma string
		if i == (len(ddi.Vars) - 1) {
			addComma = ""
		} else {
			addComma = ","
		}
		nameAndType.WriteString(fmt.Sprintf("\n\t%s%s%s %s%s\t-- %s", colEscChr, strings.ToLower(v.Name), colEscChr, typeToUse.String(), addComma, v.Label))
		ddl_table.WriteString(nameAndType.String())
	}
	ddl_table.WriteString("\n);\n\n")

	return []byte(ddl_table.String()), nil
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
// returns empty byte slice if there are no discrete variables
func (dbf *DatabaseFormatter) CreateRefTables(ddi *DataDict) []byte {
	var ddlStatement strings.Builder

	for _, v := range ddi.Vars {
		if v.Interval == "discrete" {
			tableName := "ref_" + strings.ToLower(v.Name)
			var refTable strings.Builder
			refTable.WriteString(fmt.Sprintf("CREATE TABLE %s (", tableName))
			// limit labels to 1000 characters, which should be far more than enough
			maxCharsInLab := 1000
			colType := dbf.columnType(v)
			catAndType := fmt.Sprintf("\n\tval %s,\n\tlabel %s(%d)\n);\n\n", colType, dbf.DataTypes["string"], maxCharsInLab)
			refTable.WriteString(catAndType)
			ddlStatement.WriteString(refTable.String())

			var insertStatement strings.Builder
			insertStatement.WriteString(fmt.Sprintf("INSERT INTO %s (val, label)\nVALUES", tableName))
			for i, cat := range v.Cats {
				var addComma string
				if i == (len(v.Cats) - 1) {
					addComma = "\n"
				} else {
					addComma = ","
				}
				escapedLabel := strings.ReplaceAll(cat.Label, "'", "''")
				valAndLab := fmt.Sprintf("\n\t(%s, '%s')%s", cat.Val, escapedLabel, addComma)
				insertStatement.WriteString(valAndLab)
			}
			insertStatement.WriteString(";\n\n")
			ddlStatement.WriteString(insertStatement.String())
		}
	}

	return []byte(ddlStatement.String())
}

// CreateIndices generates "CREATE INDEX idx_var" statements for a set of columns. As of now, does not
// support multi-column index creations.
//
// returns error if a column is not recognized in the data dictionary
func (dbf *DatabaseFormatter) CreateIndices(ddi *DataDict, cols []string) ([]byte, error) {
	var indexStatements strings.Builder
	varNames := dbf.VariableNames(ddi)
	for _, col := range cols {
		if !slices.Contains(varNames, strings.ToLower(col)) {
			return nil, fmt.Errorf("cannot create idx on unrecognized variable %s", col)
		}
		indexStatements.WriteString(fmt.Sprintf("CREATE INDEX idx_%s ON %s (%s);\n\n", col, dbf.TableName, col))
	}
	return []byte(indexStatements.String()), nil
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

	// get the column types once, which should slightly speed up the
	// tuple-insert-statement processing below
	colTypes := dbf.columnTypes(ddi)
	bulkInsertInit := fmt.Sprintf("INSERT INTO %s VALUES\n", dbf.TableName)

	dat := make([]byte, 0, len(buffer))
	for i := 0; i < len(buffer); i += bytesPerLine {
		row := buffer[i:(i + bytesPerLine)]
		inserts, err := dbf.insertTuple(ddi, row, colTypes)
		if err != nil {
			return nil, fmt.Errorf("error row %v: %w", row, err)
		}
		dat = append(dat, inserts...)
	}
	bulkInsertStatement := append([]byte(bulkInsertInit), dat...)
	bulkInsertStatement[len(bulkInsertStatement)-2] = ';'
	return bulkInsertStatement, nil
}

// insertTuple generates a single insertion tuple, given a row byte slice, data dictionary, and column types.
// Note that this statement does not include the insertion statement itself, as the BulkInsert method
// will be used to create insertion statements.
//
// returns error if start and end positions are not valid for row.
func (dbf *DatabaseFormatter) insertTuple(ddi *DataDict, row []byte, colTypes map[string]string) ([]byte, error) {
	var insertStatement strings.Builder
	insertStatement.WriteString("\t(")
	for i, v := range ddi.Vars {

		start, end := v.Location.Start-1, v.Location.End
		if (start < 0) || (end > len(row)) {
			return nil, fmt.Errorf("startAt %d & endAt %d not valid index range for sliceLen %d", start, end, len(row))
		}

		chars := row[start:end]
		var sChars string

		// null values
		if slices.Contains(chars, byte(' ')) {
			chars = []byte("null")
			sChars = string(chars)
			insertStatement.WriteString(sChars)
			if i != (len(ddi.Vars) - 1) {
				insertStatement.WriteString(",")
			}
			continue
		}

		switch colType := colTypes[v.Name]; colType {
		case "string":
			sChars = fmt.Sprintf("'%s'", string(chars))
		case "float":
			// for true float cases (not float due to width concerns)
			if v.DecimalPoint != 0 {
				placeDecimalAt := len(chars) - v.DecimalPoint
				chars = slices.Insert(chars, placeDecimalAt, byte('.'))
			}
			sChars = string(chars)
		case "int":
			sChars = string(chars)
			sChars = strings.TrimLeft(sChars, "0") // trim to reduce outFile sizes
			if len(sChars) == 0 {
				sChars = "0"
			}
		default:
		}

		insertStatement.WriteString(sChars)
		if i != (len(ddi.Vars) - 1) {
			insertStatement.WriteString(",")
		}
	}
	insertStatement.WriteString("),\n")
	return []byte(insertStatement.String()), nil
}

// columnTypes returns a map of variable names and their database-equivalent column types
// this function will be used to generate a map that'll be continually used to find types
// in BulkInsert calls
func (dbf *DatabaseFormatter) columnTypes(ddi *DataDict) map[string]string {
	colToType := make(map[string]string)
	for _, v := range ddi.Vars {
		colToType[v.Name] = dbf.columnType(v)
	}
	return colToType
}

// columnType is a helper function that returns the type that
// a database column should have: options include ["int", "float", "string"]
func (dbf *DatabaseFormatter) columnType(v Var) string {
	// if the variable type is a character type -> must be string
	if v.VType.VarType == "character" {
		return "string"
	}
	// if a column has decimal point places > 0 -> must be float
	// if the variable has width > 10 -> must be float (with 0 decimal places)
	if (v.DecimalPoint > 0) || (v.Location.Width > maxPlacesFori32) {
		return "float"
	}
	// return int in all other cases
	return "int"
}
