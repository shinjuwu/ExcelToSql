package base

import (
	"os"
	"fmt"
)

func Checkerr(err error){
	if err != nil{
		fmt.Println(err)
		os.Exit(-1)
	}
}