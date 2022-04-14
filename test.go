package main

import "fmt"

func main() {
	numbers := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	trim := numbers[3:]
	fmt.Print(trim[0])
}
