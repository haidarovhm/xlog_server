package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

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
			log.Println("failed to write block")
		}
		req.out <- err == nil
	}
	lgr.done <- true
}

type acceptor struct {
	ln net.Listener
	q  chan *req
	wg sync.WaitGroup
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
		log.Println("accepted")
		acc.wg.Add(1)
		go acc.handleClient(conn)
	}
}

func (acc *acceptor) handleClient(conn net.Conn) {
	defer acc.wg.Done()
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	data := make([]byte, 10)
	resp := make(chan bool)

	for {
		read, err := conn.Read(data)
		if read != 0 {
			acc.q <- &req{
				data: data,
				out:  resp,
			}
			if <-resp == false {
				break
			}
			err := writer.WriteByte(byte(1))
			if err == nil {
				err = writer.Flush()
			}
			if err != nil {
				log.Println("failed to write client")
				break
			}
		}
		if err != nil {
			if err != io.EOF {
				log.Println("failed to read client")
			} else {
				log.Println("conn is closed")
			}
			break
		}
	}
}

var trace *bool
var logfile *string

func main() {
	trace = flag.Bool("v", false, "enable tracing")
	logfile = flag.String("f", "test.xlog", "path to xlog")
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
