// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// maxBytesPerFile determines the maximum bytes (pre-processed fixed-width, not SQL statements)
// that an outFile can hold. As seen below, the current max size is 10 GiB; after testing both
// processing time and database insertion time (on the number of supported database systems), this
// value will likely be revisited.
const maxBytesPerFile = (1 << 30) * 10

// NewDumpWriter generates a new DumpWriter. It generates the number of outFiles needed, and
// the schema file. If makeItDir is true, then a directory is first created, and all files are placed
// in that directory. If makeItDir is fale, only one outFile will be created, and the outFile will necessarily
// be the same file as the schema file. Performs directory and file cleanup in case of errors in the process of
// creating outFiles.
func NewDumpWriter(totBytes int, writerName string, makeItDir bool) (DumpWriter, error) {
	// if either the default option is used, or makeItDir == false AND -o is provided:
	// need to trim the ".sql" for the rest of the function logic to work
	// note: this doesn't protect agains non-".sql" extensions.
	writerName = strings.TrimSuffix(writerName, ".sql")
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
	// the outFile will point to the same underlying file.
	outFiles := make([]*os.File, nOutFiles)
	for i := 0; i < nOutFiles; i++ {
		// if not dir format, then there's only one outFile
		// and it'll be the same as the schema file
		// we'll have to worry about file closing later on, but we can handle that
		// in functions downstream in the pipeline
		if !makeItDir {
			outFiles[i] = schemaF
			break
		}

		iName := fmt.Sprintf("inserts_%d.sql", i)
		fName := filepath.Join(writerName, iName)
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
			// remove directory created
			_ = os.Remove(writerName)
			return DumpWriter{}, err
		}
		outFiles[i] = f
	}
	// make it now
	dw := DumpWriter{SchemaFile: schemaF, OutFiles: outFiles}
	return dw, nil
}

// NewDumpWriterDDLOnly returns a new DumpWriter, meant only for DDL creation.
// As the logic is much simpler here, it warrants a
// seperate function.
func NewDumpWriterDDLOnly(fileName string) (DumpWriter, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return DumpWriter{}, err
	}
	dw := DumpWriter{SchemaFile: f, OutFiles: []*os.File{}}
	return dw, nil
}

// WriteParsedResults spawns N := len(DumpWriter.OutFiles) outFile writers to write SQL insertion
// statements to outFiles. It reads from a channel of ParsedResults, and writes successful results
// to an outFile.
//
// In case of any write errors, all created files and directories should be deleted, and the program
// should exit.
func (dw DumpWriter) WriteParsedResults(wg *sync.WaitGroup, parsedStream <-chan ParsedResult, exitFunc func(err error, topic string)) {
	wg.Add(len(dw.OutFiles))
	for _, f := range dw.OutFiles {
		go func(f *os.File) {
			defer wg.Done()
			err := writeToDump(f, parsedStream)
			// if you can't commit a write, you need to stop all actions
			// close all files, and delete them, and also exit in some way
			if err != nil {
				dw.FileCleanup() // close all files, delete everything
				exitFunc(err, "DumpWriter")
			}
		}(f)
	}
}

// WriteDDL writes main table creation, index creation, and ref_table creation and inserts to
// the DumpWriter.SchemaFile. If at any step, a write cannot be completed, a non-nil error is returned.
func (dw DumpWriter) WriteDDL(dbfmtr *DatabaseFormatter, ddi *DataDict, indices []string) error {
	// IF DIR FORMAT: once we write the DDL, we can close this file
	// IF SINGLE FILE FORMAT: we cannot close the file yet. We still have inserts to make
	// IF LEN(outFiles) == 0: we can close, as we are only generating DDL
	if len(dw.OutFiles) > 1 || len(dw.OutFiles) == 0 {
		defer dw.SchemaFile.Close()
	}
	// defer dw.SchemaFile.Close()
	// main table creation
	tableSQL, err := dbfmtr.CreateMainTable(ddi)
	if err != nil {
		return fmt.Errorf("ipums2db: table creation: %w", err)
	}
	// ref tables
	refTablesSQL := dbfmtr.CreateRefTables(ddi)
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

// FileCleanup deletes all files created, schema and/our output files
func (dw DumpWriter) FileCleanup() {
	// if single-file dump writer, close schema file first
	if len(dw.OutFiles) == 1 {
		dw.SchemaFile.Close()
	}
	// delete schema file
	_ = os.Remove(dw.SchemaFile.Name())
	// delete outFiles
	for _, f := range dw.OutFiles {
		// ensure outfiles are closed
		_ = f.Close()
		_ = os.Remove(f.Name())
	}
}

// DumpWriter writes the database SQL representation of a fixed-width file. The SchemaFile
// will represent the file where table creation, index creation, and ref_table creation and insertions
// will take place. OutFiles hold where insertion statements will take place.
type DumpWriter struct {
	SchemaFile *os.File
	OutFiles   []*os.File
}

// writeToDump reads ParsedResults from a channel, and writes the results to an output
// file. In the case of errors in the ParsedResult, the function returns with a non-nil
// error. If a parsed block of insertion statements cannot be written, the file will be closed
// and deleted, and a non-nil error is returned.
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

// numOutFiles determines, based on the size of a fixed-width file, the
// number of output files to create.
func numOutFiles(totBytes int) int {
	// Each out file should be at most maxBytesPerFile bytes
	// so if the totBytes is X bytes, we should have
	// (X / maxBytesPerFile) + (totBytes%maxBytesPerFile > 0 ? 1 : 0) outFiles
	remainderF := 0
	if totBytes%maxBytesPerFile > 0 { // almost always the case, but just in case of mod == 0
		remainderF = 1
	}
	numFiles := (totBytes / maxBytesPerFile) + remainderF
	return numFiles
}
