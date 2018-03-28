package ignite

import (
	"bytes"
)

// Response is struct of response data
type Response struct {
	Len     int32
	UID     int64
	Status  int32
	Message string

	Data *bytes.Reader
}

// ReadPrimitives reads primitives from buffered response data
func (r *Response) ReadPrimitives(data ...interface{}) error {
	return readPrimitives(r.Data, data...)
}

// ReadObjects reads objects from buffered response data
func (r *Response) ReadObjects(count int) ([]interface{}, error) {
	return readObjects(r.Data, count)
}
