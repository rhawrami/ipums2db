// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import (
	"fmt"
	"strings"
)

const (
	POSTGRES string = "postgres"
	ORACLE   string = "oracle"
	MYSQL    string = "mysql"
	MSSQL    string = "mssql"
)

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
		types2DBtypes["float"] = "number"
		types2DBtypes["string"] = "varchar2(4000)"
		return types2DBtypes, nil
	default:
		return nil, fmt.Errorf("unrecognized database type %s not in {'postgres', 'oracle', 'mysql', 'postgres'}", dbType)
	}
}

func MakeNewDBFormatter(dbType string) (*DataBaseFormatter, error) {
	dataTypes, err := getDataTypes(dbType)
	if err != nil {
		return nil, err
	}

	return &DataBaseFormatter{DbType: dbType, DataTypes: dataTypes}, nil
}

type DataBaseFormatter struct {
	DbType    string
	DataTypes map[string]string
}

func (dbf *DataBaseFormatter) CreateMainTable(ddi *DataDict, tableName string) ([]byte, error) {
	init_statement := fmt.Sprintf("CREATE TABLE %s (", tableName)
	ddl_table := init_statement

	for _, v := range ddi.Vars {
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
		nameAndType = fmt.Sprintf("\n\t%v %v,\t-- %v", strings.ToLower(v.Name), typeToUse, v.Label)
		ddl_table += nameAndType
	}
	ddl_table += "\n);\n"

	return []byte(ddl_table), nil
}

func (dbf *DataBaseFormatter) CreateRefTables(ddi *DataDict) ([]byte, error) {
	ddlStatement := ""
	for _, v := range ddi.Vars {
		if v.Interval == "discrete" {
			tableName := "ref_" + strings.ToLower(v.Name)
			init_statement := fmt.Sprintf("CREATE TABLE %s (", tableName)
			refTable := init_statement

			catAndType := fmt.Sprintf("\n\tval %v,\n\tlabel %v\n);\n\n", dbf.DataTypes["int"], dbf.DataTypes["string"])
			refTable += catAndType
			ddlStatement += refTable

			insertStatement := fmt.Sprintf("INSERT INTO %v (val, label)\nVALUES", tableName)
			for _, cat := range v.Cats {
				valAndLab := fmt.Sprintf("\n\t(%v, '%v')", cat.Val, cat.Label)
				insertStatement += valAndLab
			}
			insertStatement += ";\n\n"
			ddlStatement += insertStatement
		}
	}
	return []byte(ddlStatement), nil
}

// func (dbf *DataBaseFormatter) InsertStatement() ([]byte, error) {}
