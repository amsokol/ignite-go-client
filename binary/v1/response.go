package ignite

import (
	"bytes"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// Response is struct of response data
type Response struct {
	Len     int32
	UID     int64
	Status  int32
	Message string

	Data *bytes.Reader
}

// CheckStatus checks status of operation execution.
// Returns:
// nil in case of success.
// error object in case of operation failed.
func (r *Response) CheckStatus() error {
	if r.Status != errors.StatusSuccess {
		return errors.NewError(r.Status, r.Message)
	}
	return nil
}

// ReadPrimitives reads primitives from buffered response data
func (r *Response) ReadPrimitives(data ...interface{}) error {
	return readPrimitives(r.Data, data...)
}

// ReadObject reads object from buffered response data
func (r *Response) ReadObject() (interface{}, error) {
	return readObject(r.Data)
}

// ReadObjects reads objects from buffered response data
func (r *Response) ReadObjects(count int) ([]interface{}, error) {
	return readObjects(r.Data, count)
}
