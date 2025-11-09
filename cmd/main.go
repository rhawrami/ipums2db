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
	flag.StringVar(&dbType, "b", "postgres", "database type")
	flag.StringVar(&ddiPath, "x", "", "XML path (MANDATORY)")
	flag.StringVar(&tabName, "t", "ipums_tab", "main table name")
	flag.StringVar(&indices, "i", "", "indices to create; comma-delim for multiple")
	flag.BoolVar(&makeItDir, "d", false, "make directory output format")
	flag.StringVar(&outFile, "o", "ipums_dump.sql", "output file/dir name")
	flag.BoolVar(&silentProg, "s", false, "silence output")
	// usage
	flag.Usage = printUsage
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
	go 棕熊.PrintLoadingMessage(silentProg) // technically never closes/terminates, but it's fine

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
	// in case of any write errors, delete files/directories and exit immediately
	dw.WriteParsedResults(&writerWG, parsedBlockStream, checkErr)

	// wait on groups
	jobMakerWG.Wait()
	parserWG.Wait()
	writerWG.Wait()

	// end summary ----------------------------------------
	end := time.Now()
	棕熊.PrintFinalSummary(silentProg, start, end, int(totBytes))
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
		fmt.Printf("ipums2db: must pass path to XML file (e.x. -x cps_001.xml)\nsee --help for more\n")
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
		fmt.Printf("ipums2db: args: only provide one argument (path to .dat file)\nsee --help for more\n")
		os.Exit(2)
	}
	if len(args) == 0 {
		fmt.Printf("ipums2db: args: provide path to .dat file\nsee --help for more\n")
		os.Exit(2)
	}
}

// printUsage prints usage of ipums2db
// this will need to be manually updated for future command updates,
// but I think it's worth it
func printUsage() {
	usageStatement := `Usage: %s [options...] -x <xml> <dat>
Flags:
 -x <xml>                     DDI XML path (mandatory)
 -b <dbType>                  Database type (default 'postgres')
 -t <tabName>                 Table name (default 'ipums_tab')
 -i <idx1[,idx2]>             Variable[s] to index on (default no idx)
 -d                           Make directory format (default false)
 -o <outFileOrDir>            File/Directory to output (default 'ipums_dump.sql')
 -s                           Silent output (default false)

Full Usage Example:
 %s -b mysql -t mytab -i age,sex -o mydump.sql -x myACS.xml myACS.dat
For more information, visit https://github.com/rhawrami/ipums2db
`
	fmt.Printf(usageStatement, os.Args[0], os.Args[0])
}
