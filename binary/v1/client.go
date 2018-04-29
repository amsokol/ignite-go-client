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

	// Do sends request and receives response
	Do(req Request, res Response) error

	// Close closes connection.
	// Returns:
	// nil in case of success.
	// error object in case of error.
	Close() error

	// Cache Configuration methods
	// See for details:
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations

	// CacheCreateWithName Creates a cache with a given name.
	// Cache template can be applied if there is a '*' in the cache name.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_create_with_name
	CacheCreateWithName(cache string) error

	// CacheGetOrCreateWithName creates a cache with a given name.
	// Cache template can be applied if there is a '*' in the cache name.
	// Does nothing if the cache exists.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_get_or_create_with_name
	CacheGetOrCreateWithName(cache string) error

	// CacheGetNames returns existing cache names.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_get_names
	CacheGetNames() ([]string, error)

	// CacheGetConfiguration gets configuration for the given cache.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_get_configuration
	CacheGetConfiguration(cache string, flag byte) (*CacheConfiguration, error)
}

type client struct {
	debugID string
	conn    net.Conn
	mutex   *sync.Mutex

	Client
}

// IsConnected return true if connection to the cluster is active
func (c *client) Connected() bool {
	return c.conn != nil
}

// Do sends request and receives response
func (c *client) Do(req Request, res Response) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// send request
	if _, err := req.WriteTo(c.conn); err != nil {
		return errors.Wrapf(err, "failed to send request to server")
	}

	// receive response
	_, err := res.ReadFrom(c.conn)

	return err
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

	c := &client{conn: conn, debugID: strings.Join([]string{"network=", network, "', address='", address, "'"}, ""),
		mutex: &sync.Mutex{}}
	runtime.SetFinalizer(c, clientFinalizer)

	// request and response
	req := NewRequestHandshake(major, minor, patch)
	res := &ResponseHandshake{}

	// make handshake
	if err = c.Do(req, res); err != nil {
		c.Close()
		return nil, errors.Wrapf(err, "failed to make handshake")
	}

	if !res.Success {
		c.Close()
		return nil, errors.Errorf("handshake failed: %s, server supported protocol version is v%d.%d.%d",
			res.Message, res.Major, res.Minor, res.Patch)
	}

	// return connected client
	return c, nil
}

// clientFinalizer is resource leak spy
func clientFinalizer(c *client) {
	if c.Connected() {
		debug.ResourceLeakLogger.Printf("client \"%s\" is not closed", c.debugID)
		c.Close()
	}
}
