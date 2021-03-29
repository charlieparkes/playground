package main

import "golang.org/x/tour/pic"

func Pic(dx, dy int) [][]uint8 {
	img := make([][]uint8, dy)
	for i, _ := range img {
		v := make([]uint8, dx)
		for j, _ := range v {
			v[j] = uint8(i^j)
		}
		img[i] = v
	}
	return img
}

func main() {
	pic.Show(Pic)
}
