package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	棕熊 "github.com/rhawrami/ipums2db/internal"
)

func main() {
	// flags ----------------------------------------
	var (
		dbType     string
		ddiPath    string
		tabName    string
		indices    string
		outFile    string
		makeItDir  bool
		silentProg bool
	)
	flag.StringVar(&dbType, "db", "postgres", "database type; ex. -d postgres")
	flag.StringVar(&ddiPath, "x", "", "XML path; ex. -x cps_001.xml")
	flag.StringVar(&tabName, "t", "ipums_tab", "main table name; ex. -t cps_respondents")
	flag.StringVar(&indices, "i", "", "indices to create (comma delimit for >1 idx); ex. -i age; ex. -i age,sex")
	flag.StringVar(&outFile, "o", "ipums_dump.sql", "output file name; ex. -o cps_dump.sql")
	flag.BoolVar(&makeItDir, "d", false, "make directory output format")
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

	start := time.Now() // start time here; prior to file creations

	// setup ----------------------------------------
	// get totalBytes in the datFile
	totBytes, err := 棕熊.TotalBytes(datFileName)
	checkErr(err, "totBytes")

	// gen new DatabaseFormatter
	dbfmtr, err := 棕熊.NewDBFormatter(dbType, tabName)
	checkErr(err, "DBFormatter")

	// gen new DataDict
	ddi, err := 棕熊.NewDataDict(ddiPath)
	checkErr(err, "DataDict")

	// gen new DumpWriter
	dw, err := 棕熊.NewDumpWriter(totBytes, outFile, makeItDir)
	checkErr(err, "DumpWriter")

	// gen new JobConfig
	// MaxBytesPerJob: the max byte size that a single parser (writer) will parse (write)
	// NumParsers: number of concurrent parsers
	// ParsedResChanSize: size of buffered ParsedResult channel
	nWriters := len(dw.OutFiles)
	jCFG := 棕熊.NewJobConfig(totBytes, nWriters)
	maxBperJob, nParsers, nBuffRes := jCFG.MaxBytesPerJob, jCFG.NumParsers, jCFG.ParsedResChanSize

	// bytes per row in datFile
	bPerR := 棕熊.BytesPerRow(&ddi)

	// gen new DatParser
	dp := 棕熊.NewDatParser(datFileName, nParsers, &ddi, dbfmtr)

	// job submission summary ----------------------------------------
	棕熊.PrintJobSummary(silentProg, "=", dbType, tabName, indices, ddiPath, datFileName)
	// print loading message
	go 棕熊.PrintLoadingMessage(silentProg)

	// write ddl
	// note: this includes table and index creations, as well as ref_table[s] creation and inserts
	err = dw.WriteDDL(dbfmtr, &ddi, idx)
	checkErr(err, "write DDL")

	// channels and waitgroups ----------------------------------------
	// jobStream: channel of ParsingJobs that will be consumed by DatParser[s]
	// parsedBlockStream: buffered channel of ParsedResults that will be consumed by DumpWriter[s]
	jobStream := make(chan 棕熊.ParsingJob)
	parsedBlockStream := make(chan 棕熊.ParsedResult, nBuffRes)
	// gen waitgroups; one for each of the three steps
	var jobMakerWG, parserWG, writerWG sync.WaitGroup

	// goroutines ----------------------------------------
	// spawn a single JobMaker
	jobMakerWG.Add(1)
	go func() {
		defer jobMakerWG.Done()
		err := 棕熊.MakeParsingJobsStream(bPerR, int(totBytes), maxBperJob, jobStream)
		checkErr(err, "parsing")
	}()

	// spawn parser[s]
	dp.ParseBlocks(&parserWG, jobStream, parsedBlockStream)
	// close parsedBlockStream when parsers are done consuming from jobStream
	go func() {
		parserWG.Wait()
		close(parsedBlockStream)
	}()

	// spawn writer[s]
	dw.WriteParsedResults(&writerWG, parsedBlockStream)

	// wait on groups
	jobMakerWG.Wait()
	parserWG.Wait()
	writerWG.Wait()

	// end summary ----------------------------------------
	end := time.Now()
	棕熊.PrintFinalSummary(silentProg, start, end, int(totBytes), outFile)
}

// Helper Functions
// checkErr checks if err != nil; prints error and exits if so
func checkErr(err error, topic string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v: %v\n", topic, err)
		os.Exit(1)
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
