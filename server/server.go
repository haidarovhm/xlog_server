package main

import (
	"io"
	"log"
	"net"
	"os"
)

type req struct {
	data []byte
	resp chan bool
}

type logger struct {
	file *os.File
	reqs chan *req
}

func newLogger(path string) (*logger, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &logger{
		file: file,
		reqs: make(chan *req),
	}, nil
}

func (lgr *logger) log(data []byte) error {
	_, err := lgr.file.Write(data)
	return err
}

func (lgr *logger) run() {
	for req := range lgr.reqs {
		err := lgr.log(req.data)
		if err != nil {
			log.Println("failed to log block")
		}
		req.resp <- err == nil
	}
}

func handleClient(conn net.Conn, reqs chan *req) {
	defer conn.Close()
	data := make([]byte, 10)
	resp := make(chan bool)

	for {
		read, err := conn.Read(data)
		if read != 0 {
			reqs <- &req{
				data: data,
				resp: resp,
			}
			if <-resp == false {
				break
			}
			_, err := conn.Write([]byte("ok\n"))
			if err != nil {
				log.Println("failed to write client")
				break
			}
		}
		if err != nil {
			if err != io.EOF {
				log.Println("failed to read client")
			}
			break
		}
	}
}

func main() {
	lgr, err := newLogger("/tmp/test.xlog")
	if err != nil {
		log.Fatal(err)
	}

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	go lgr.run()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go handleClient(conn, lgr.reqs)
	}
}
