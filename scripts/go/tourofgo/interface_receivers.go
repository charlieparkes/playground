package main

import (
	"fmt"
	"math"
)

type MyInterface interface {
	Abs() float64
}

type Vertex struct {
	X, Y float64
}

func (v Vertex) Abs() float64 {
	val := math.Sqrt(v.X*v.X + v.Y*v.Y)
	fmt.Println(val)
	return val
}

func test(i MyInterface) {
	fmt.Printf("%T.Abs() => %v \n", i, i.Abs())
}

func main() {
	v := Vertex{1, 2}

	header("v")
	test(v)
	describe(v)

	header("&v")
	test(&v)
	describe(&v)

	var x MyInterface

	header("x = v")
	x = v
	test(x)
	describe(x)

	header("x = &v")
	x = &v
	test(x)
	describe(x)

	// x = v
	// x.Abs()

	// x = &v
	// x.Abs()
}

func describe(i MyInterface) {
	fmt.Printf("Describe Interface (%v, %T)\n", i, i)
}

func header(s string) {
	fmt.Printf("\n%v\n", s)
}