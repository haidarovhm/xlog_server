package main

import(
	"fmt"
	"os"
	"log"
)

type xlog struct {
	file *os.File
}

func newXlog(path string) (*xlog, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	log := &xlog{
		file: file,
	}
	return log, nil
}

func (log *xlog) log(data []byte) error {
	_, err := log.file.Write(data)
	return err
}

func main() {
	xlog, err := newXlog("")
	if err != nil {
		log.Fatal(err)
	}
	err = xlog.log([]byte("Hi, there!\n"))
	if err != nil {
		log.Fatal(err)
	}
	err = xlog.log([]byte("Hi. What's up\n"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Ok")
}
