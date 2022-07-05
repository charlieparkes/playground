package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
)

type Coord struct {
	i, j int
}

func main() {
	f, err := os.Open("1.input")
	if err != nil {
		panic(err)
	}

	scanner := csv.NewReader(f)
	scanner.TrimLeadingSpace = true
	scanner.Comma = ' '
	records, err := scanner.ReadAll()
	if err != nil {
		panic(err)
	}
	var ints []int
	var lines [][]int
	for _, record := range records {
		var line []int
		for _, s := range record {
			x, err := strconv.Atoi(s)
			if err != nil {
				panic(err)
			}
			line = append(line, x)
			ints = append(ints, x)
		}
		lines = append(lines, line)
	}
	for _, line := range lines {
		fmt.Println(line)
	}
	sort.Ints(ints)
	fmt.Println(ints)
}
