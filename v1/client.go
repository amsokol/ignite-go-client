package ignite

import (
	"net"
)

// Options is connection options
type Options struct {
	version Version
}

// Version is protocol version
type Version struct {
	Major int16
	Minor int16
	Patch int16
}

// Client is interface to communicate with Apache Ignite cluster
type Client interface {
	Exec(code int16, uid int64, data ...interface{}) (Response, error)

	Prepare(code int16, uid int64) (Operation, error)
	Call(operation Operation) (Response, error)

	Begin(length int32, code int16, uid int64) error
	Write(data ...interface{}) error
	Commit() (Response, error)

	Close() error
}

type client struct {
	options Options
	conn    net.Conn

	Client
}

/*
func (c *client) call(operation int16, uid int64, params ...interface{}) (Response, error) {

}

func (c *client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// NewClient returns client is connected to the Apache Ignite cluster
func NewClient(network, address string, options Options) (Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, errors.New("failed to open connection: " + err.Error())
	}

	c := client{options: options, conn: conn}
}
*/
