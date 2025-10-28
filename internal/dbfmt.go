// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

func MakeNewDBFormatter() *DataBaseFormatter {}

type DataBaseFormatter struct {
	ApplyLabels bool
}

func (dbf *DataBaseFormatter) CreateTable() ([]byte, error) {}

func (dbf *DataBaseFormatter) InsertStatement() ([]byte, error) {}
