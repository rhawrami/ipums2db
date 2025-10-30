// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

import (
	"fmt"
	"os"
)

func GenerateOffsets(filePath string, rowLength int, retRowN int, offsetStream chan<- []Offset) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	fileStat, err := file.Stat()
	if err != nil {
		return err
	}
	byteCount := int(fileStat.Size())
	fmt.Println(byteCount)
	// nRows := byteCount / rowLength
	// bytesPerOffset := retRowN * rowLength

	defer file.Close()
	offsetSlice := make([]Offset, 0)
	for i := 0; i < byteCount; i += rowLength {
		if len(offsetSlice) == retRowN {
			fmt.Printf("%+v\n", offsetSlice)
			offsetStream <- offsetSlice           // send off set of offsets
			offsetSlice = make([]Offset, retRowN) // make new set
		}
		offset := Offset{Start: i, Stop: (i + rowLength - 2)}
		offsetSlice = append(offsetSlice, offset)
	}
	return nil
}

// Offset contains the starting and ending byte to be read from an IPUMS fixed-width file
type Offset struct {
	Start int
	Stop  int
}
