package main

import (
	"fmt"
	"testing"
)

func TestMap(t *testing.T) {
	a := make(map[string]string)

	a["a"] = "2"
	a["b"] = "3"

	for k, v := range a {
		fmt.Printf("%v %v\n", k, v)
	}
}
