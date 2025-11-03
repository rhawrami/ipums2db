package internal

import (
	"fmt"
	"os"
)

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

type ParsedResult struct {
	Block    []byte
	AnyError error
}

type ParserConfig struct {
	DatFileName  string
	JobStream    chan ParsingJob
	ParsedStream chan ParsedResult
}
