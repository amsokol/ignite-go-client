package internal

import (
	"net"
)

func GetTestConnection() (net.Conn, error) {
	return net.Dial("tcp", "127.0.0.1:10800")
}
