package main

import (
	"bufio"
	"example.com/test/utils"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const ioTimeoutSec = 90

type req struct {
	data []byte
	out  chan bool
}

type logger struct {
	file *os.File
	in   chan *req
	done chan bool
}

func newLogger(path string) (*logger, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &logger{
		file: file,
		in:   make(chan *req),
		done: make(chan bool),
	}, nil
}

func (lgr *logger) log(data []byte) error {
	_, err := lgr.file.Write(data)
	if err == nil {
		err = lgr.file.Sync()
	}
	return err
}

func (lgr *logger) run() {
	for req := range lgr.in {
		err := lgr.log(req.data)
		if err != nil {
			log.Println(err)
		}
		req.out <- err == nil
	}
	lgr.done <- true
}

type acceptor struct {
	ln     net.Listener
	q      chan *req
	wg     sync.WaitGroup
	nextId uint
}

func newAcceptor(addr string, q chan *req) (*acceptor, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &acceptor{
		ln: ln,
		q:  q,
	}, nil
}

func (acc *acceptor) run() {
	defer acc.wg.Done()
	acc.wg.Add(1)
	quit := false

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		log.Println("shutting down...")
		signal.Stop(sigs)

		quit = true
		acc.ln.Close()
		acc.wg.Wait()
		close(acc.q)
	}()

	for {
		conn, err := acc.ln.Accept()
		if quit {
			break
		}
		if err != nil {
			log.Println(err)
			continue
		}

		id := acc.nextId
		acc.nextId++
		if *verbose {
			log.Printf("%d accepted\n", id)
		}
		acc.wg.Add(1)
		go acc.handleClient(conn, id)
	}
}

func (acc *acceptor) handleClient(conn net.Conn, id uint) {
	defer acc.wg.Done()
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	data := make([]byte, *blockSize)
	resp := make(chan bool)

	for {
		utils.SetConnTimeout(conn, ioTimeoutSec)
		_, err := io.ReadFull(conn, data)
		if err != nil {
			if err != io.EOF {
				log.Printf("%d block reading failed, closing\n", id)
			} else if *verbose {
				log.Printf("%d closed\n", id)
			}
			break
		}

		acc.q <- &req{
			data: data,
			out:  resp,
		}
		if <-resp == false {
			log.Printf("%d block writing failed, closing\n", id)
			break
		}

		utils.SetConnTimeout(conn, ioTimeoutSec)
		err = writer.WriteByte(byte(1))
		if err == nil {
			err = writer.Flush()
		}
		if err != nil {
			log.Printf("%d block acking failed, closing\n", id)
			break
		}
	}
}

var verbose *bool
var logfile *string
var blockSize *uint

func main() {
	verbose = flag.Bool("v", false, "verbose logging")
	logfile = flag.String("f", "test.xlog", "path to xlog")
	blockSize = flag.Uint("b", 4096, "block size")
	flag.Parse()

	addr := ":8080"
	if flag.NArg() != 0 {
		addr = flag.Args()[0]
	}

	lgr, err := newLogger(*logfile)
	if err != nil {
		log.Fatal(err)
	}

	acc, err := newAcceptor(addr, lgr.in)
	if err != nil {
		log.Fatal(err)
	}

	go lgr.run()
	go acc.run()
	<-lgr.done
}
