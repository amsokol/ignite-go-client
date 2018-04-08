package ignite

import (
	"context"
	"fmt"
	"net"
)

func getTestClient() (*client, error) {
	c, err := NewClient(context.Background(), "tcp", "127.0.0.1:10800", 1, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %s", err.Error())
	}
	original, _ := c.(*client)
	return original, nil
}

func getTestConnection() (net.Conn, error) {
	return net.Dial("tcp", "127.0.0.1:10800")
}
