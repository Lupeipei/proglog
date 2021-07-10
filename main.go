package main

import "fmt"

type person struct {
  age int
  name string
}

func main() {
  var m = map[int]person{
    1: person{11, "abc"},
    2: person{12, "abcd"},
  }

  var mm = map[int]*person{}

  for k, v := range m {
    fmt.Println(k)
    fmt.Println(v)
    mm[k] = &v
  }
  fmt.Println(mm[1], mm[2])
}
