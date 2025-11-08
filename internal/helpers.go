// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"
)

// maxBytesofDatFileInMemory determines the maximum byte count of fixed-width file data in memory,
// assumine that every parser and writer goroutine is working on a block of data at the same time.
// As of now, it is set at 100 MiB, but this value will be revisited.
const maxBytesofDatFileInMemory = (1 << 20) * 100

// NewJobConfig returns a JobConfig that will be used to determine the max bytes processed
// per parsing job, the size of the parsed results buffered channel, and the number of
// parsers. A number of arbitrary decisions are made, but they should work for a number of
// different users. Hopefully :)
func NewJobConfig(totBytes int, nWriters int) JobConfig {
	// decide on NumParsers
	// there should be 5 parsers at max and 2 parsers at minimum; writes will be the bottleneck.
	// note that this is an arbitrary selection, but 5 performs pretty well.
	MINPARSERS, MAXPARSERS := 2, 5
	nCPU := runtime.NumCPU()
	nParsers := 1
	if nCPU > nParsers {
		nCPUsSaveParseWrite := nCPU - nWriters - nParsers
		if nCPUsSaveParseWrite > 0 {
			chooseFrom := []int{nCPUsSaveParseWrite, MAXPARSERS}
			nParsers = slices.Min(chooseFrom)
		} else {
			nParsers = MINPARSERS
		}
	}
	// ParsedResChanrSize will just be the size of nParsers
	parsedResChanSize := nParsers
	// decide on MaxBytesPerJob
	// at any given moment, at most I'd like there to be at most maxBytesofDatFileInMemory bytes
	// of the dat file in memory. This means that, the max number of bytes
	// processed in each parse job should be maxBytesofDatFileInMemory // (nParsers + nWriters),
	// as both the  parsers and writers could both be processing/recieving
	// at the same moment.
	maxBPerJ := maxBytesofDatFileInMemory / (nParsers + nWriters)

	return JobConfig{
		ParsedResChanSize: parsedResChanSize,
		NumParsers:        nParsers,
		MaxBytesPerJob:    maxBPerJ,
	}
}

// A JobConfig determines the size of the parsed results buffered channel, the
// number of parsers to be spawned, and the max number of bytes that each parser
// should be processing.
type JobConfig struct {
	ParsedResChanSize int
	NumParsers        int
	MaxBytesPerJob    int
}

// TotalBytes returns the total bytes in the fixed width file.
// Returns err if file cannot be opened.
func TotalBytes(datFileName string) (int, error) {
	datFile, err := os.Open(datFileName)
	if err != nil {
		return 0, err
	}
	defer datFile.Close()

	stats, err := datFile.Stat()
	if err != nil {
		return 0, err
	}
	totBytes := stats.Size()
	return int(totBytes), nil
}

// PrintFinalSummary prints the time elapsed for a parsing job, as well as the MiB parsed per second
func PrintFinalSummary(silent bool, start, end time.Time, totBytes int, dumpFile string) {
	if silent {
		return
	}
	timeElapsed := end.Sub(start).Round(time.Millisecond)
	bytesInMiB := 1 << 20
	MiBPerSec := float64(totBytes) / timeElapsed.Seconds() / float64(bytesInMiB)
	fmt.Printf("\rTime elapsed: %v (%.2f MiB/s)\nDump written to: %s\n", timeElapsed, MiBPerSec, dumpFile)
}

// PrintJobSummary prints the summary for a program run.
// if silent, then the summary is not printed.
func PrintJobSummary(silent bool, delim, dbT, tabN, idx, ddi, datFN string) {
	if silent {
		return
	}
	delimLong := strings.Repeat(delim, len(datFN)+5) // includes the "dat: " chars, so add 5
	fmt.Printf(
		"%s\ndbT: %s\ntab: %s\nidx: %s\nxml: %s\ndat: %s\n%s\n",
		delimLong, dbT, tabN, idx, ddi, datFN, delimLong,
	)
}

// PrintLoadingMessage prints a loading message while the program runs.
// Prints nothng if silent.
// Should be ran as a goroutine.
func PrintLoadingMessage(silent bool) {
	if silent {
		return
	}
	printStatement := []byte("I-P-U-M-S-!")
	downTime := time.Millisecond * 400
	clearSpaces := strings.Repeat(" ", len(printStatement))
	for {
		for i := range printStatement {
			fmt.Printf("\r%s", string(printStatement[:(i+1)]))
			time.Sleep(downTime)
		}
		fmt.Printf("\r%s", clearSpaces)
	}
}
