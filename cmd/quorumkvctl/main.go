package main

import (
	"fmt"
	"os"

	"github.com/darshmahadevia/ternion/internal/cli"
)

func main() {
	if err := cli.Run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
