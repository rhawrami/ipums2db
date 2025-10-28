package main

import (
	"encoding/xml"
	"fmt"
	"os"

	pasta "github.com/rhawrami/ipums2db/internal"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Printf("%s: Must provide arg\n", os.Args[0])
		os.Exit(1)
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var IpumsDDI pasta.DataDict
	decoder := xml.NewDecoder(f)
	err = decoder.Decode(&IpumsDDI)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, v := range IpumsDDI.Vars {
		fmt.Printf("%v :: %v\n", v.Label, v.DecimalPoint)
	}

	// buffer := make([]byte, 500)
	// n, err := f.Read(buffer)
	// if err != nil {
	// 	if err != io.EOF {
	// 		fmt.Println(err)
	// 		os.Exit(1)
	// 	}
	// }

	// lineLength := 96
	// numLines := lineLength / len(buffer)
	// for i := 0; i < numLines; i++ {
	// 	fmt.Printf("%v", string(buffer[(lineLength*i):(lineLength*(i+1))]))
	// }
	// for _, v := range buffer[:n] {
	// 	fmt.Printf("%v", string(v))
	// }
}
