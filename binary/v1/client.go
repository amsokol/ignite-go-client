package ignite

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/amsokol/ignite-go-client/binary/errors"
	"github.com/amsokol/ignite-go-client/debug"
)

// Client is interface to communicate with Apache Ignite cluster.
// Client is thread safe.
type Client interface {
	// Connected return true if connection to the cluster is active
	Connected() bool

	// Close closes connection.
	// Returns:
	// nil in case of success.
	// error object in case of error.
	Close() error
}

type client struct {
	debugID string
	conn    net.Conn
	lock    *sync.Mutex

	Client
}

// IsConnected return true if connection to the cluster is active
func (c *client) Connected() bool {
	return c.conn != nil
}

// Close closes connection.
// Returns:
// nil in case of success.
// error object in case of error.
func (c *client) Close() error {
	if c.Connected() {
		defer func() { c.conn = nil }()
		return c.conn.Close()
	}
	return nil
}

// Connect connects to the Apache Ignite cluster
// Returns: client
func Connect(ctx context.Context, network, host string, port, major, minor, patch int) (Client, error) {
	address := fmt.Sprintf("%s:%d", host, port)

	// connect
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open connection")
	}

	// handshake
	req, err := NewRequestHandshake(major, minor, patch)
	if err != nil {
		conn.Close()
		return nil, errors.Wrapf(err, "failed to create handshake request")
	}
	_, err = req.WriteTo(conn)
	if err != nil {
		conn.Close()
		return nil, errors.Wrapf(err, "failed to send handshake request")
	}
	res, err := NewResponseHandshake(conn)
	if err != nil {
		conn.Close()
		return nil, errors.Wrapf(err, "failed to receive handshake response")
	}
	if !res.Success() {
		conn.Close()
		return nil, errors.Errorf("handshake failed: %s, server supported protocol version is v%d.%d.%d",
			res.Message(), res.Major(), res.Minor(), res.Patch())
	}

	// return connected client
	c := &client{conn: conn, debugID: strings.Join([]string{"network=", network, "', address='", address, "'"}, ""),
		lock: &sync.Mutex{}}
	runtime.SetFinalizer(c, clientFinalizer)

	return c, nil
}

// clientFinalizer is resource leak spy
func clientFinalizer(c *client) {
	if c.Connected() {
		debug.ResourceLeakLogger.Printf("client \"%s\" is not closed", c.debugID)
		c.Close()
	}
}
