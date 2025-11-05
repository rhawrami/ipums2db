package internal

import (
	"fmt"
	"os"
)

// WriteToDumpFile reads from a channel of parsed blocks, and then writes those
// blocks to an output file. The channel it reads from contains ParsedResults,
// which can contain errors. In case of errors, it prints out the error and continues.
//
// Returns error if a block write  cannot be completed.
func WriteToDumpFile(outFile *os.File, parsedStream chan ParsedResult) error {
	for res := range parsedStream {
		if res.AnyError != nil {
			return fmt.Errorf("encountered error parsing: %w", res.AnyError)
		}
		_, err := outFile.Write(res.Block)
		if err != nil {
			outFile.Close()
			errRM := os.Remove(outFile.Name())
			if errRM != nil {
				return fmt.Errorf("encountered error deleting corrupted file: %v", errRM)
			}
			return fmt.Errorf("encountered error writing: %v; deleting in-progress dump file", err)
		}
	}
	outFile.Close()
	return nil
}

// ParseBlock wraps the DatabaseFormatter.BulkInsert method, now reading from a channel of
// ParsingJobs, and writing to a channel of ParsedResults. Each ParseBlock call gets a file header.
func ParseBlock(dbfmtr DatabaseFormatter, ddi *DataDict, cfg ParserConfig) {
	datFile, err := os.Open(cfg.DatFileName)
	if err != nil {
		cfg.ParsedStream <- ParsedResult{Block: nil, AnyError: err}
		return
	}
	defer datFile.Close()
	for job := range cfg.JobStream {
		parsedBlock, err := dbfmtr.BulkInsert(ddi, datFile, job.StartAtRow, job.RowsToRead)
		cfg.ParsedStream <- ParsedResult{Block: parsedBlock, AnyError: err}
	}
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
