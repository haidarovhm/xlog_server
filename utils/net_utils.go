package utils

import (
	"net"
	"time"
)

func SetConnTimeout(conn net.Conn, sec int) {
	tout := time.Now().Add(time.Duration(sec) * time.Second)
	conn.SetDeadline(tout)
}

