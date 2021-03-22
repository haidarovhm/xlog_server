package main

import (
	"log"
	"net"
	"math/rand"
	"bufio"
	"flag"
)

const blockSize = 10

func fillRandom(b []byte) {
	rnd := rand.Intn(256)
	for i := range b {
		b[i] = byte(rnd);
	}
}

func getData() []byte {
	data := make([]byte, blockSize)
	fillRandom(data)
	return data
}

var blocksNum *uint

func main() {
	blocksNum = flag.Uint("n", 5, "blocks num to send")
	flag.Parse()

	addr := ":8080"
	if flag.NArg() != 0 {
		addr = flag.Args()[0]
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	reader := bufio.NewReader(conn);
	pending := make([][]byte, 0, *blocksNum)

	for i := 0; i < int(*blocksNum); i++ {
		data := getData()
		_, err := conn.Write(data)
		if err != nil {
			log.Fatal(err)
		}
		pending = append(pending, data)

		stat, err := reader.ReadByte()
		if err != nil {
			log.Fatal(err)
		}
		if stat == 1 {
			pending = pending[1:]
		}
		// log.Printf("%X: %d", data, stat)
	}
	log.Printf("failed packets num: %d", len(pending))
}
