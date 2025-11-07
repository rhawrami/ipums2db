// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import (
	"os"
	"sync"
)

// NewDatParser returns a DatParser given
// a fixed-width file path, the number of parsers to spawn,
// a DataDict to read from, and a DatabaseFormatter to parse results with
func NewDatParser(datFileName string, nParsers int, ddi *DataDict, dbfmtr *DatabaseFormatter) DatParser {
	return DatParser{
		datFileName: datFileName,
		nParsers:    nParsers,
		ddi:         ddi,
		dbfmtr:      dbfmtr,
	}
}

// ParseBlocks spawns N := nParsers goroutines, each goroutine generating their own *os.File header; each parser
// reads jobs from a ParsingJob stream, parses results, and sends ParsedResults to an output channel.
//
// In case of file open errors, the goroutine returns (may come back to this mechanism). In case of parsing errors, the
// errors will be handled by the DumpWriter reading ParsedResults from the output stream.
func (dp DatParser) ParseBlocks(wg *sync.WaitGroup, jobStream <-chan ParsingJob, parsedStream chan<- ParsedResult) {
	wg.Add(dp.nParsers)
	for i := 0; i < dp.nParsers; i++ {
		go func() {
			defer wg.Done()
			datFile, err := os.Open(dp.datFileName)
			if err != nil {
				return // come back to this
			}
			defer datFile.Close()
			for job := range jobStream {
				parsedBlock, err := dp.dbfmtr.BulkInsert(dp.ddi, datFile, job.StartAtRow, job.RowsToRead)
				parsedStream <- ParsedResult{Block: parsedBlock, AnyError: err}
			}
		}()
	}
}

// DatParser spawns parsers to convert rows of fixed-width file data into SQL insertion statements
// when ParseBlocks is ran, N := nParsers goroutines are spawned to consume ParsingJobs and send ParsedResults
type DatParser struct {
	datFileName string
	nParsers    int
	ddi         *DataDict
	dbfmtr      *DatabaseFormatter
}

// A ParsedResult contains a block of fixed-width data parsed to SQL inserts,
// and an error if applicable.
type ParsedResult struct {
	Block    []byte
	AnyError error
}

// A ParserConfig contains the name of a fixed-width file, a channel of jobs that a ParseBlock call
// should read from, and a channel of parsed results that a ParseBlock call should write to.
type ParserConfig struct {
	DatFileName  string
	JobStream    chan ParsingJob
	ParsedStream chan ParsedResult
}
