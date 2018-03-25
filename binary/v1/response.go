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

func (r *Response) Read(data ...interface{}) error {
	return read(r.Data, data...)
}
