// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import (
	"os"
	"sync"
)

func NewDatParser(datFileName string, nParsers int, ddi *DataDict, dbfmtr *DatabaseFormatter) DatParser {
	return DatParser{
		datFileName: datFileName,
		nParsers:    nParsers,
		ddi:         ddi,
		dbfmtr:      dbfmtr,
	}
}

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
	wg.Wait()
	close(parsedStream)
}

type DatParser struct {
	datFileName string
	nParsers    int
	ddi         *DataDict
	dbfmtr      *DatabaseFormatter
}
