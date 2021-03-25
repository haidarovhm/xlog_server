package main

import (
	"example.com/test/utils"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const ioTimeoutSec = 90

type req struct {
	data   [][]byte
	status chan error
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

func (lgr *logger) run() {
	for req := range lgr.in {
		var err error
		for i := range req.data {
			_, err = lgr.file.Write(req.data[i])
			if err != nil {
				log.Println(err)
				break
			}
		}
		if err == nil {
			err = lgr.file.Sync()
		}
		req.status <- err
	}
	lgr.done <- true
}

type acceptor struct {
	ln     net.Listener
	syncQ  chan *req
	wg     sync.WaitGroup
	nextId uint
}

func newAcceptor(addr string, syncQ chan *req) (*acceptor, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &acceptor{
		ln:    ln,
		syncQ: syncQ,
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
		close(acc.syncQ)
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

func runReader(conn net.Conn, blocks chan []byte, status chan error) {
	for {
		data := make([]byte, *blockSize)
		utils.SetConnTimeout(conn, ioTimeoutSec)
		_, err := io.ReadFull(conn, data)
		if err != nil {
			status <- err
			break
		}
		blocks <- data
	}
}

func (acc *acceptor) handleClient(conn net.Conn, id uint) {
	defer acc.wg.Done()
	defer conn.Close()

	resp := make([]byte, int(*syncSize))
	for i := range resp {
		resp[i] = byte(1)
	}
	syncBlocks := make([][]byte, 0, len(resp))
	inBlocks := make(chan []byte)
	inEOF := make(chan error)
	syncStat := make(chan error)

	go runReader(conn, inBlocks, inEOF)
	for cont := true; cont; {
		select {
		case block := <-inBlocks:
			syncBlocks = append(syncBlocks, block)
			if len(syncBlocks) < cap(syncBlocks) {
				continue
			}
		case err := <-inEOF:
			if err != io.EOF {
				log.Printf("%d blocks reading failed, closing\n", id)
				return
			}
			if *verbose {
				log.Printf("%d closed\n", id)
			}
			cont = false
		// TODO: fix timer recreation for idle connections
		case <-time.After(time.Duration(100) * time.Millisecond):
		}
		if len(syncBlocks) == 0 {
			continue
		}

		acc.syncQ <- &req{
			data:   syncBlocks,
			status: syncStat,
		}
		if <-syncStat != nil {
			log.Printf("%d blocks writing failed, closing\n", id)
			return
		}

		utils.SetConnTimeout(conn, ioTimeoutSec)
		_, err := conn.Write(resp[0:len(syncBlocks)])
		if err != nil {
			log.Printf("%d blocks acking failed, closing\n", id)
			return
		}
		syncBlocks = syncBlocks[:0]
	}
}

var verbose *bool
var logfile *string
var blockSize *uint
var syncSize *uint

func main() {
	verbose = flag.Bool("v", false, "verbose logging")
	logfile = flag.String("f", "test.xlog", "path to xlog")
	blockSize = flag.Uint("b", 4096, "block size")
	syncSize = flag.Uint("n", 250, "blocks num per sync")
	flag.Parse()

	if *blockSize == 0 || *syncSize == 0 {
		log.Fatal("bad args")
	}

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
