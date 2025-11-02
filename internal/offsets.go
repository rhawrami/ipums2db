// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import "fmt"

// MakeParsingJobsStream ParsingJobs to a channel that a DatabaseFormatter will consume to
// parse and generate bulk "INSERT INTO tab VALUES ...".
//
// Takes in the bytesPerRow of the fixed width file (chars + newline), the totBytes of the file, and
// the maxBytesPerJob that are allowed to be parsed. The maxBytesPerJob determines the buffer size
// allocated for reading the specified lines.
//
// The maxBytesPerJob is the only variable not already determined by the input file. Given that the file
// will most often parsed in parallel, and the buffer size is allocated based on this input, a large limit
// with a combination of N parser goroutines at any one time could mean N * maxBytesPerJob of memory allocated
// to storing the file contents at any one time. For small files, this will not be a concern. But imagine 7 spawned
// parser goroutines each parsing, at any given moment, 262144000 bytes (250 MiB), meaning ~1.70 GiB of memory.
func MakeParsingJobsStream(bytesPerRow, totBytes, maxBytesPerJob int, jobsStream chan ParsingJob) error {
	if maxBytesPerJob > totBytes {
		return fmt.Errorf("maxBytesPerJob (%d) cannot be greater than totBytes (%d)", maxBytesPerJob, totBytes)
	}
	if maxBytesPerJob < bytesPerRow {
		return fmt.Errorf("maxBytesPerJob (%d) cannot be less than bytesPerRow (%d)", maxBytesPerJob, bytesPerRow)
	}
	if bytesPerRow > totBytes {
		return fmt.Errorf("bytesPerRow (%d) cannot be greater than totBytes (%d)", bytesPerRow, totBytes)
	}

	totRows := totBytes / bytesPerRow
	rowsPerJob := maxBytesPerJob / bytesPerRow
	// nJobs := totRows / rowsPerJob

	defer close(jobsStream)
	onRow := 0
	for onRow <= totRows {
		if rowsPerJob >= (totRows - onRow) {
			lastJob := ParsingJob{onRow, (totRows - onRow)}
			jobsStream <- lastJob
			break
		}
		job := ParsingJob{onRow, rowsPerJob}
		jobsStream <- job
		onRow += rowsPerJob
	}
	return nil
}

// ParsingJob represents a file parsing set that a DatabaseFormatter
// needs to parse through.
//
// The job requires a DatabaseFormatter to start
// reading at row StartAtRow, and read through RowsToRead rows.
type ParsingJob struct {
	StartAtRow int
	RowsToRead int
}
