package main

import (
	"fmt"

	"github.com/hextechpal/prio/core"
)

func main() {
	_ = core.NewPrio(nil)
	fmt.Printf("Prio Initialized")
}
