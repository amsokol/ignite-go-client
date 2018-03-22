package ignite

// Response is interface to get response data
type Response interface {
	Length() int32
	UID() int64
	Status() int32
	Error() string

	Read(data ...interface{}) error
}

type response struct {
	length int32
	uid    int64
	status int32
	error  string

	Response
}
