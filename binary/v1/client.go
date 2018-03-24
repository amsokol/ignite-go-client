package ignite

import (
	"fmt"
	"io"
	"net"
)

const (
	// StatusSuccess means success
	StatusSuccess = 0
)

// Response is struct of response data
type Response struct {
	Len     int32
	UID     int64
	Status  int32
	Message string
}

// Client is interface to communicate with Apache Ignite cluster.
// Client is not thread-safe.
type Client interface {
	Exec(code int16, uid int64, data ...interface{}) (Response, error)

	Prepare(code int16, uid int64) *Operation
	Call(o *Operation) (Response, error)

	Begin(length int32, code int16, uid int64) error
	Write(data ...interface{}) error
	Commit() (Response, error)
	Read(data ...interface{}) error

	Close() error

	// Cache Configuration methods
	CacheCreateWithName(name string, status *int32) error
	CacheDestroy(name string, status *int32) error
	CacheGetOrCreateWithName(name string, status *int32) error
	CacheGetNames(status *int32) ([]string, error)
}

type client struct {
	conn net.Conn

	Client
}

// Close closes connection
func (c *client) Close() error {
	if c.conn != nil {
		defer func() { c.conn = nil }()
		return c.conn.Close()
	}
	return nil
}

// Exec executes request
func (c *client) Exec(code int16, uid int64, data ...interface{}) (Response, error) {
	o := c.Prepare(code, uid)
	// write data
	if err := o.Write(data...); err != nil {
		return Response{}, fmt.Errorf("failed to write request data to operation: %s", err.Error())
	}
	return c.Call(o)
}

func (c *client) Prepare(code int16, uid int64) *Operation {
	return &Operation{Code: code, UID: uid}
}

func (c *client) Call(o *Operation) (Response, error) {
	// send request header
	if err := c.Begin(int32(2+8+o.Data.Len()), o.Code, o.UID); err != nil {
		return Response{}, fmt.Errorf("failed to send request header: %s", err.Error())
	}
	// send request body
	if err := c.Write(o.Data.Bytes()); err != nil {
		return Response{}, fmt.Errorf("failed to send request body: %s", err.Error())
	}
	return c.Commit()
}

// Start request
func (c *client) Begin(length int32, code int16, uid int64) error {
	return write(c.conn, length, code, uid)
}

// Write request data
func (c *client) Write(data ...interface{}) error {
	return write(c.conn, data...)
}

// Commit request and return response
func (c *client) Commit() (Response, error) {
	var r Response
	if err := read(c.conn, &r.Len, &r.UID, &r.Status); err != nil {
		return r, fmt.Errorf("failed to read response header: %s", err.Error())
	}
	if r.Status != StatusSuccess {
		// Response status
		if err := read(c.conn, &r.Message); err != nil {
			return r, fmt.Errorf("failed to read error message: %s", err.Error())
		}
	}
	return r, nil
}

// Read response data
func (c *client) Read(data ...interface{}) error {
	return read(c.conn, data...)
}

// NewClient100 connects to the Apache Ignite cluster by protocol version v1.0.0
func NewClient100(network, address string) (Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %s", err.Error())
	}
	if err = handshake(conn, 1, 0, 0); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to make handshake: %s", err.Error())
	}
	return &client{conn: conn}, nil
}

// handshake - besides socket connection, the thin client protocol requires a connection handshake to ensure
// that client and server versions are compatible. Note that handshake must be the first message
// after connection establishment.
func handshake(rw io.ReadWriter, major int16, minor int16, patch int16) error {
	// Send handshake request
	if err := write(rw,
		// Message length
		int32(8),
		// Handshake operation
		byte(1),
		// Protocol version, e.g. 1,0,0
		major, minor, patch,
		// Client code
		byte(2),
	); err != nil {
		return fmt.Errorf("failed to send handshake request: %s", err.Error())
	}

	// Receive handshake response
	var length int32
	var res byte
	if err := read(rw, &length, &res); err != nil {
		return fmt.Errorf("failed to read handshake response (length and result): %s", err.Error())
	}
	if res != 1 {
		var msg string
		if err := read(rw, &major, &minor, &patch, &msg); err != nil {
			return fmt.Errorf("failed to read handshake response (supported protocol version and error message): %s",
				err.Error())
		}
		return fmt.Errorf("handshake failed, code=%d, message='%s', supported protocol version is v%d.%d.%d",
			res, msg, major, minor, patch)
	}

	return nil
}
