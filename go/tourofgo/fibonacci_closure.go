package main

import "fmt"

// fibonacci is a function that returns
// a function that returns an int.
func fibonacci() func() int {
	l1 := 0
	l2 := 0
	return func() int {
		next := l1 + l2
		l1 = l2
		if next == 0 {
			l2 = 1
		} else {
			l2 = next
		}
		return next
	}
}

func main() {
	f := fibonacci()
	for i := 0; i < 10; i++ {
		fmt.Println(f())
	}
}
