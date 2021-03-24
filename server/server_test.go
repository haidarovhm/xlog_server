package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func prep(t *testing.T) (*logger, *os.File) {
	lgr, err := newLogger("tmp.xlog")
	if err != nil {
		t.FailNow()
	}
	file, err := os.Open("tmp.xlog")
	if err != nil {
		t.FailNow()
	}
	return lgr, file
}

func readAll(t *testing.T, file *os.File) []byte {
	file.Seek(0, 0)
	buf := make([]byte, 100)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		t.FailNow()
	}
	return buf[:n]
}

func TestLogNew(t *testing.T) {
	_, file := prep(t)
	if buf := readAll(t, file); len(buf) != 0 {
		t.Error("log read failed")
	}
}

func TestLogAppend(t *testing.T) {
	lgr, file := prep(t)

	str := []byte("Hi")
	exp := []byte("Hi")
	lgr.log(str)
	buf := readAll(t, file)
	if !bytes.Equal(buf, exp) {
		t.Error("log write check failed")
	}

	str = []byte(", there!")
	exp = []byte("Hi, there!")
	lgr.log(str)
	buf = readAll(t, file)
	if !bytes.Equal(buf, exp) {
		t.Error("log append check failed")
	}
}
