package main

import (
	"fmt"
	"os"
	"sync"

	i2db "github.com/rhawrami/ipums2db/internal"
)

func CreateDumpFile(dumpFileName string) (*os.File, error) {
	file, err := os.Create(dumpFileName)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func WriteSchemaAndIndices(dumpFile *os.File, dbfmtr *i2db.DatabaseFormatter, ddi *i2db.DataDict, indices []string) error {
	// main table creation
	tableSQL, err := dbfmtr.CreateMainTable(ddi)
	if err != nil {
		return fmt.Errorf("ipums2db: table creation: %w", err)
	}
	// ref tables
	refTablesSQL, err := dbfmtr.CreateRefTables(ddi)
	if err != nil {
		return fmt.Errorf("ipums2db: reference tables creation: %w", err)
	}
	// indices
	indicesSQL, err := dbfmtr.CreateIndices(ddi, indices)
	if err != nil {
		return fmt.Errorf("ipums2db: index creation: %w", err)
	}

	buffer := make([]byte, 0, len(tableSQL)+len(refTablesSQL)+len(indicesSQL))
	buffer = append(buffer, tableSQL...)
	buffer = append(buffer, refTablesSQL...)
	buffer = append(buffer, indicesSQL...)

	_, err = dumpFile.Write(buffer)
	if err != nil {
		return fmt.Errorf("ipums2db: DDL write: %v", err)
	}

	return nil
}

func main() {
	// File creations; table and index creations
	//
	// dat file
	datFileName := "testdata/short_cps.dat"
	datFile, err := os.Open(datFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ipums2db: %v\n", err)
		os.Exit(1)
	}
	// open the ddi; make DataDict
	xmlPath := "testdata/cps_00244.xml"
	ddi, err := i2db.NewDataDict(xmlPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ipums2db: %v\n", err)
		os.Exit(1)
	}
	// make DBFMTR
	dbtype := "postgres"
	tablename := "ipums_table"
	dbfmtr, err := i2db.NewDBFormatter(dbtype, tablename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ipums2db: formatter: %v\n", err)
		os.Exit(1)
	}
	// make outFile
	outFileName := "out.sql"
	dumpFile, err := CreateDumpFile(outFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ipums2db: outFile: %v\n", err)
		os.Exit(1)
	}
	// defer dat and out close
	defer dumpFile.Close()
	defer datFile.Close()
	// write main table creation, reference tables creation/insert, indices
	err = WriteSchemaAndIndices(dumpFile, dbfmtr, &ddi, []string{"age", "sex"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ipums2db: DDL: %v\n", err)
		os.Exit(1)
	}

	// get total bytes
	stats, err := datFile.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ipums2db: error finding totBytes: %v\n", err)
		os.Exit(1)
	}
	totBytes := stats.Size()
	bytesPerRow := i2db.BytesPerRow(&ddi)
	// chans
	jobStream := make(chan i2db.ParsingJob)
	parsedBlockStream := make(chan i2db.ParsedResult, 3)
	// process
	var wg sync.WaitGroup
	// spawn JobMaker
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := i2db.MakeParsingJobsStream(bytesPerRow, int(totBytes), 500, jobStream)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ipums2db: parsing: %v\n", err)
			os.Exit(1)
		}
	}()
	// spawn Parsers
	pConfig := i2db.ParserConfig{
		DatFileName:  datFileName,
		JobStream:    jobStream,
		ParsedStream: parsedBlockStream,
	}
	numParsers := 3
	wg.Add(numParsers)
	go func() {
		for i := 0; i < numParsers; i++ {
			go func() {
				defer wg.Done()
				i2db.ParseBlock(*dbfmtr, &ddi, pConfig)
			}()
		}
		close(parsedBlockStream)
	}()

	// spawn Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := i2db.WriteToDumpFile(dumpFile, parsedBlockStream)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ipums2db: writer: %v\n", err)
			os.Exit(1)
		}
	}()

	// wait on all workers
	wg.Wait()
}
