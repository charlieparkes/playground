package main

import (
	"fmt"
	"math"
)

type Geometry interface {
	area() float64
	perim() float64
}

type Rect struct {
	width, height float64
}

type Circle struct {
	radius float64
}

func (r Rect) area() float64 {
	return r.width * r.height
}

func (c Circle) area() float64 {
	return math.Pi * c.radius * c.radius
}

func (r Rect) perim() float64 {
	return r.width * 2 + r.height * 2
}

func (c Circle) perim() float64 {
	return 2 * math.Pi * c.radius
}

func describe(g Geometry) {
	fmt.Printf("%T%v, Area: %v, Perimeter: %v\n", g, g, g.area(), g.perim())
}

func main() {
	r := Rect{2, 5}
	c := Circle{10}

	describe(r)
	describe(c)
}
