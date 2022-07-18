package main

import "fmt"

func main() {
	var a []int
	a = append(a, 1, 2)
	var b []int
	b = nil
	fmt.Println(len(a))
	a = append(a, nil...)
	a = append(a, b...)
	fmt.Println(a)
	fmt.Println(len(a))
	fmt.Println(b)
}
