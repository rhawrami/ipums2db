package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	棕熊 "github.com/rhawrami/ipums2db/internal"
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

	// print job summary
	棕熊.PrintJobSummary(silentProg, "=", dbType, tabName, indices, ddiPath, datFileName)
	// print loading message
	棕熊.PrintLoadingMessage(silentProg)

	// new DataDict and DatabaseFormatter
	ddi, dbfmtr, err := 棕熊.NewDataDictAndDatabaseFormatter(dbType, tabName, ddiPath)
	checkErr(err, "ipums2db: DataDict/DBFormatter")

	// get dat file and total bytes in file
	totBytes, err := 棕熊.TotalBytes(datFileName)
	checkErr(err, "ipums2db: .dat file")
	// get total bytes
	bytesPerRow := 棕熊.BytesPerRow(&ddi)

	// make outFile
	dumpFile, err := 棕熊.CreateDumpFile(outFile)
	checkErr(err, "ipums2db: dumpFile")

	// write main table creation, reference tables creation/insert, indices
	err = 棕熊.WriteDDL(dumpFile, dbfmtr, &ddi, idx)
	checkErr(err, "ipums2db: DDL")

	// goroutines
	// nWriters
	nWriters := 1 // come back to this
	// job config
	jobConfig := 棕熊.NewJobConfig(totBytes, nWriters)
	// parsing config
	maxBPerJob := jobConfig.MaxBytesPerJob
	parsedBlocksChanSize := jobConfig.ParsedResChanSize
	nParsers := jobConfig.NumParsers

	// chans
	jobStream := make(chan 棕熊.ParsingJob)
	parsedBlockStream := make(chan 棕熊.ParsedResult, parsedBlocksChanSize)
	// waitgroups
	var makeJobWG, parseWG, writeWG sync.WaitGroup

	// spawn JobMaker
	makeJobWG.Add(1)
	go func() {
		defer makeJobWG.Done()
		err := 棕熊.MakeParsingJobsStream(bytesPerRow, int(totBytes), maxBPerJob, jobStream)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ipums2db: parsing: %v\n", err)
			os.Exit(1)
		}
	}()

	// spawn Parsers
	pConfig := 棕熊.ParserConfig{
		DatFileName:  datFileName,
		JobStream:    jobStream,
		ParsedStream: parsedBlockStream,
	}
	parseWG.Add(nParsers)
	for i := 0; i < nParsers; i++ {
		go func() {
			defer parseWG.Done()
			棕熊.ParseBlock(*dbfmtr, &ddi, pConfig)
		}()
	}

	// close parsed stream
	go func() {
		parseWG.Wait()
		close(parsedBlockStream)
	}()

	// spawn Writer
	writeWG.Add(nWriters)
	for i := 0; i < nWriters; i++ {
		go func() {
			defer writeWG.Done()
			err := 棕熊.WriteToDumpFile(dumpFile, parsedBlockStream)
			checkErr(err, "ipums2db: writer")
		}()
	}

	// wait on each of the three steps
	parseWG.Wait()
	makeJobWG.Wait()
	writeWG.Wait()

	end := time.Now()
	棕熊.PrintFinalSummary(silentProg, start, end, int(totBytes), outFile)
}

// Helper Functions
// checkErr checks if err != nil; prints error and exits if so
func checkErr(err error, topic string) {
	if err != nil {
		log.Fatalf("%v: %v\n", topic, err)
	}
}

// checkDDIFlag checks if the ddi path is empty
func checkDDIFlag(ddiF string) {
	if len(ddiF) == 0 {
		fmt.Printf("ipums2db: -ddi: must pass path to XML file (e.x. -ddi cps_001.xml)\n")
		os.Exit(2)
	}
}

// parseIndicesFlag returns the comma-delimited indices flag argument as a string slice
func parseIndicesFlag(indF string) []string {
	if len(indF) == 0 {
		return []string{}
	}
	indices := strings.Split(indF, ",")
	return indices
}

// checkOneArg checks if either there is more than one argument provided, or if no arguments are provided
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
