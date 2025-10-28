// Package internal provides all functionality for ipums2db
// from data-dictionary parsing to SQL statement creation
package internal

// DataDict represents an IPUMS xml-decoded data dictionary
type DataDict struct {
	Vars []Var `xml:"dataDscr>var"` // variables included in the extract
}

// Var represents a variable included in the IPUMS data extract
type Var struct {
	Name         string    `xml:"name,attr"`   // "readable" variable name
	Label        string    `xml:"labl"`        // actual variable name
	VType        VarFormat `xml:"varFormat"`   // variable type
	DecimalPoint string    `xml:"dcml,attr"`   // implied decimal point, if any
	Interval     string    `xml:"intrvl,attr"` // interval type (discrete v. continuous)
	Location     Loc       `xml:"location"`    // location within line
	Cats         []Cat     `xml:"catgry"`      // if discrete, values/labels per category
}

// Loc represents the location of a variable within the fixed-width line
type Loc struct {
	Start int `xml:"StartPos,attr"` // starting position in line
	End   int `xml:"EndPos,attr"`   // ending position in line
	Width int `xml:"width,attr"`    // width of variable in character count
}

// Category represents a discrete category for a variable
type Cat struct {
	Val   string `xml:"catValu"` // coded value
	Label string `xml:"labl"`    // corresponding label for coded value
}

// VarFormat represents a variables format/type
type VarFormat struct {
	VarType string `xml:"type,attr"` // variable type
}
