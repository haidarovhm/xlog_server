package main

import (
	"bufio"
	"example.com/test/utils"
	"flag"
	"log"
	"math/rand"
	"net"
)

const ioTimeoutSec = 90

func fillRandom(b []byte) {
	rnd := rand.Intn(256)
	for i := range b {
		b[i] = byte(rnd)
	}
}

func runSender(conn net.Conn, out chan []byte) {
	for i := 0; i < int(*blocksNum); i++ {
		data := make([]byte, *blockSize)
		fillRandom(data)
		_, err := conn.Write(data)
		if err != nil {
			log.Println("block sending failed, closing")
			break
		}
		out <- data
	}
	close(out)
}

func runReceiver(conn net.Conn, out chan byte) {
	reader := bufio.NewReader(conn)
	for i := 0; i < int(*blocksNum); i++ {
		utils.SetConnTimeout(conn, ioTimeoutSec)
		stat, err := reader.ReadByte()
		if err != nil {
			log.Println("ack receiving failed, closing")
			break
		}
		if stat != 1 {
			log.Println("block storing failed, closing")
			break
		}
		out <- stat
	}
	close(out)
}

var blocksNum *uint
var blockSize *uint

func main() {
	blocksNum = flag.Uint("n", 5, "blocks num to send")
	blockSize = flag.Uint("b", 4096, "block size")
	flag.Parse()

	if *blocksNum == 0 || *blockSize == 0 {
		log.Fatal("bad args")
	}

	addr := ":8080"
	if flag.NArg() != 0 {
		addr = flag.Args()[0]
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	pending := make([][]byte, 0, *blocksNum)
	blocks := make(chan []byte)
	stats := make(chan byte)
	var sentBlocks uint = 0
	var ackedBlocks uint = 0

	go runSender(conn, blocks)
	go runReceiver(conn, stats)
loop:
	for {
		select {
		case block, ok := <-blocks:
			if ok {
				sentBlocks++
				pending = append(pending, block)
			} else {
				blocks = nil
			}
		case _, ok := <-stats:
			if !ok {
				break loop
			}
			pending = pending[1:]
			ackedBlocks++
		}
	}
	log.Println("sent", sentBlocks, "acked", ackedBlocks)
}
