package ignite

import (
	"fmt"
	"net"
)

func getTestClient() (*client, error) {
	c, err := NewClient100("tcp", "127.0.0.1:10800")
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %s", err.Error())
	}
	original, _ := c.(*client)
	return original, nil
}

func getTestConnection() (net.Conn, error) {
	return net.Dial("tcp", "127.0.0.1:10800")
}
