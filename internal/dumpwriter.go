// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

func NewDumpWriter(totBytes int, writerName string, makeItDir bool) (DumpWriter, error) {
	// calc num outfiles
	nOutFiles := 1
	if makeItDir {
		nOutFiles = numOutFiles(totBytes)
	}
	// make new dir
	if makeItDir {
		// make new dir
		var perms os.FileMode = 0755
		err := os.Mkdir(writerName, perms)
		if err != nil {
			return DumpWriter{}, err
		}
	}
	// make schema file
	schemaFName := fmt.Sprintf("%s.sql", writerName)
	if makeItDir {
		schemaFName = filepath.Join(writerName, "ddl.sql")

	}
	schemaF, err := os.Create(schemaFName)
	if err != nil {
		// clean up directory made
		if makeItDir {
			_ = os.Remove(writerName)
		}
		return DumpWriter{}, err
	}
	// make outFiles
	// note that if there's only one outfile, then the schemaFile and
	// the OutFile will point to the same underlying file.
	outFiles := make([]*os.File, nOutFiles)
	for i := 0; i < nOutFiles; i++ {
		fName := fmt.Sprintf("%s.sql", writerName)
		if makeItDir {
			iName := fmt.Sprintf("inserts_%d.sql", i)
			fName = filepath.Join(writerName, iName)
		}
		f, err := os.Create(fName)
		if err != nil {
			// delete all files in case of errors
			for j := 0; j < i; j++ {
				_ = outFiles[j].Close()
				errRM := os.Remove(outFiles[j].Name())
				if errRM != nil {
					return DumpWriter{}, errRM // if this happens, you're out of luck pal
				}
			}
			return DumpWriter{}, err
		}
		outFiles[i] = f
	}
	// make it now
	dw := DumpWriter{SchemaFile: schemaF, OutFiles: outFiles}
	return dw, nil
}

func (dw DumpWriter) WriteParsedResults(wg *sync.WaitGroup, parsedStream <-chan ParsedResult) error {
	wg.Add(len(dw.OutFiles))
	for _, f := range dw.OutFiles {
		go func(f *os.File) {
			defer wg.Done()
			err := writeToDump(f, parsedStream)
			if err != nil {
				return
			}
		}(f)
	}
	wg.Wait()
	return nil
}

func (dw DumpWriter) WriteDDL(dbfmtr *DatabaseFormatter, ddi *DataDict, indices []string) error {
	// once we write the DDL, we can close this file
	defer dw.SchemaFile.Close()
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

	lenDDL := len(tableSQL) + len(refTablesSQL) + len(indicesSQL)
	buffer := make([]byte, 0, lenDDL)
	// append DDL
	buffer = append(buffer, tableSQL...)
	buffer = append(buffer, refTablesSQL...)
	buffer = append(buffer, indicesSQL...)

	_, err = dw.SchemaFile.Write(buffer)
	if err != nil {
		return fmt.Errorf("ipums2db: DDL write: %v", err)
	}
	return nil
}

type DumpWriter struct {
	SchemaFile *os.File
	OutFiles   []*os.File
}

func writeToDump(outFile *os.File, parsedStream <-chan ParsedResult) error {
	for res := range parsedStream {
		if res.AnyError != nil {
			return fmt.Errorf("encountered error parsing: %w", res.AnyError)
		}
		_, err := outFile.Write(res.Block)
		if err != nil {
			outFile.Close()
			_ = os.Remove(outFile.Name())
			return fmt.Errorf("encountered error writing: %v; deleting in-progress dump file", err)
		}
	}
	outFile.Close()
	return nil
}

// NumOutFiles determines, based on the size of a dat file, the number of
func numOutFiles(totBytes int) int {
	// Each out file should be at most 10 gigabytes
	// so if the totBytes is 12 gb, we should have 2 out files
	bytesIn1GiB := 1 << 30
	maxBytesPerFile := bytesIn1GiB * 10
	remainderF := 0
	if totBytes%maxBytesPerFile > 0 { // almost always the case, but just in case of mod == 0
		remainderF = 1
	}
	numFiles := (totBytes / bytesIn1GiB) + remainderF
	return numFiles
}
