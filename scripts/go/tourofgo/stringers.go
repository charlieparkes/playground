// https://tour.golang.org/methods/18

package main

import "fmt"

type IPAddr [4]byte


// Using ip *IPAddr breaks this somehow? Dots disappear
func (ip *IPAddr) String() string {
	return fmt.Sprintf("%v.%v.%v.%v", ip[0], ip[1], ip[2], ip[3])
}

func main() {
	hosts := map[string]IPAddr{
		"loopback":  {127, 0, 0, 1},
		"googleDNS": {8, 8, 8, 8},
	}
	for name, ip := range hosts {
		fmt.Printf("%v: %v\n", name, ip)
	}

	ip := IPAddr{1, 1, 1, 1}
	fmt.Printf("%v\n", &ip)
}
