# https://tour.golang.org/methods/18

package main

import "fmt"

type IPAddr [4]byte

func (ip *IPAddr) String() string {
	tmp := make([]interface{}, len(ip))
	for i, v := range ip {
		tmp[i] = v
	}
	return fmt.Sprintf("%v.%v.%v.%v", tmp...)
}

func main() {
	hosts := map[string]IPAddr{
		"loopback":  {127, 0, 0, 1},
		"googleDNS": {8, 8, 8, 8},
	}
	for name, ip := range hosts {
		fmt.Printf("%v: %v\n", name, ip)
	}
}
