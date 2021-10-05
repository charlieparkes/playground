package main

import (
	"fmt"
)

// Go detects returned pointers and persists the values.
// This is called pointer escape analysis.

func test() *MyStruct {
	x := MyStruct{1, 2}
	return &x
}

func main() {
	fmt.Println(*test())
}