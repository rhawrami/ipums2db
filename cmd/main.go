package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	i2db "github.com/rhawrami/ipums2db/internal"
)

func main() {
	// flags
	var (
		dbType     string
		ddiPath    string
		tabName    string
		indices    string
		outFile    string
		silentProg bool
	)
	flag.StringVar(&dbType, "db", "postgres", "database type; ex. -db postgres")
	flag.StringVar(&ddiPath, "x", "", "XML path; ex. -x cps_001.xml")
	flag.StringVar(&tabName, "t", "ipums_tab", "main table name; ex. -t cps_respondents")
	flag.StringVar(&indices, "i", "", "indices to create (comma delimit for >1 idx); ex. -i age; ex. -i age,sex")
	flag.StringVar(&outFile, "o", "ipums_dump.sql", "output file name; ex. -o cps_dump.sql")
	flag.BoolVar(&silentProg, "s", false, "silent output; ex. -s")
	// parse flags
	flag.Parse()
	// check if DDI path isn't empty
	checkDDIFlag(ddiPath)
	// get indices
	idx := parseIndicesFlag(indices)
	// args
	cmdArgs := flag.Args()
	// ensure only one argument is provided
	checkOneArg(cmdArgs)
	datFileName := cmdArgs[0]

	start := time.Now()
	func() {
		if silentProg {
			return
		}
		delim := "-----------------------------"
		fmt.Printf(
			"%s\ndbT: %s\ntab: %s\nidx: %s\nxml: %s\ndat: %s\n%s\n",
			delim, dbType, tabName, indices, ddiPath, datFileName, delim,
		)
	}()
	// print load
	go func(silent bool) {
		if silent {
			return
		}
		printStatement := []byte("I-P-U-M-S-!")
		downTime := time.Millisecond * 400
		clearSpaces := strings.Repeat(" ", len(printStatement))
		for {
			for i := range printStatement {
				fmt.Printf("\r%s", string(printStatement[:(i+1)]))
				time.Sleep(downTime)
			}
			fmt.Printf("\r%s", clearSpaces)
		}
	}(silentProg)
	// File creations; table and index creations
	//
	// dat file
	datFile, err := os.Open(datFileName)
	checkErr(err, "ipums2db: datFile")
	// open the ddi; make DataDict
	ddi, err := i2db.NewDataDict(ddiPath)
	checkErr(err, "ipums2db: ddiFile")
	// make DBFMTR
	dbfmtr, err := i2db.NewDBFormatter(dbType, tabName)
	checkErr(err, "ipums2db: formatter")
	// make outFile
	dumpFile, err := createDumpFile(outFile)
	checkErr(err, "ipums2db: dumpFile")
	// defer dat and out close
	defer dumpFile.Close()
	defer datFile.Close()
	// write main table creation, reference tables creation/insert, indices
	err = writeSchemaAndIndices(dumpFile, dbfmtr, &ddi, idx)
	checkErr(err, "ipums2db: DDL")

	// get total bytes
	stats, err := datFile.Stat()
	checkErr(err, "ipums2db: totBytes error")
	totBytes := stats.Size()
	bytesPerRow := i2db.BytesPerRow(&ddi)
	// chans
	jobStream := make(chan i2db.ParsingJob)
	parsedBlockStream := make(chan i2db.ParsedResult, 6)
	// waitgroups
	var makeJobWG sync.WaitGroup
	var parseWG sync.WaitGroup
	var writeWG sync.WaitGroup
	// spawn JobMaker
	makeJobWG.Add(1)
	go func() {
		defer makeJobWG.Done()
		err := i2db.MakeParsingJobsStream(bytesPerRow, int(totBytes), 1_000_000, jobStream)
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
	numParsers := 5
	parseWG.Add(numParsers)
	for i := 0; i < numParsers; i++ {
		go func() {
			defer parseWG.Done()
			i2db.ParseBlock(*dbfmtr, &ddi, pConfig)
		}()
	}

	// close parsed stream
	go func() {
		parseWG.Wait()
		close(parsedBlockStream)
	}()

	// spawn Writer
	writeWG.Add(1)
	go func() {
		defer writeWG.Done()
		err := i2db.WriteToDumpFile(dumpFile, parsedBlockStream)
		checkErr(err, "ipums2db: writer")
	}()

	parseWG.Wait()
	makeJobWG.Wait()
	writeWG.Wait()

	end := time.Now()
	printFinalSummary(silentProg, start, end, int(totBytes), outFile)
}

func printFinalSummary(silent bool, start, end time.Time, totBytes int, dumpFile string) {
	if silent {
		return
	}
	timeElapsed := end.Sub(start).Round(time.Millisecond)
	bytesInMiB := (2 << 19)
	MiBPerSec := float64(totBytes) / timeElapsed.Seconds() / float64(bytesInMiB)
	fmt.Printf("\rTime elapsed: %v (%.2f MiB/s)\nDump written to: %s\n", timeElapsed, MiBPerSec, dumpFile)
}

func checkErr(err error, topic string) {
	if err != nil {
		log.Fatalf("%v: %v\n", topic, err)
	}
}

func createDumpFile(dumpFileName string) (*os.File, error) {
	file, err := os.Create(dumpFileName)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func writeSchemaAndIndices(dumpFile *os.File, dbfmtr *i2db.DatabaseFormatter, ddi *i2db.DataDict, indices []string) error {
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

func checkDDIFlag(ddiF string) {
	if len(ddiF) == 0 {
		fmt.Printf("ipums2db: -ddi: must pass path to XML file (e.x. -ddi cps_001.xml)\n")
		os.Exit(2)
	}
}

func parseIndicesFlag(indF string) []string {
	if len(indF) == 0 {
		return []string{}
	}
	indices := strings.Split(indF, ",")
	return indices
}

func checkOneArg(args []string) {
	if len(args) > 1 {
		fmt.Printf("ipums2db: args: only provide one argument (path to .dat file)\n")
		os.Exit(2)
	}
	if len(args) == 0 {
		fmt.Printf("ipums2db: args: provide path to .dat file\n")
		os.Exit(2)
	}
}
