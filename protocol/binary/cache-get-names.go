package binary

import (
	"io"
	"math/rand"
	"strconv"

	"github.com/amsokol/ignite-go-client/protocol/binary/internal"

	stderr "errors"
	"github.com/amsokol/go-errors"
)

// CacheGetNames returns existing cache names.
func CacheGetNames(rw io.ReadWriter) (Result, []string, error) {
	var res Result
	id := rand.Int63()

	if err := internal.Write(rw, opCacheGetNames, id); err != nil {
		return res, []string{}, errors.Wrapf(err, "failed to write operation request")
	}

	// get name count
	var count internal.Int
	if err := internal.Read(rw, true, &id, &res.Status, &res.Message, &count); err != nil {
		return res, []string{}, errors.Wrapf(err, "failed to read operation response")
	}

	if res.Status != StatusSuccess {
		return res, []string{}, stderr.New(res.Message)
	}

	// get names
	names := make([]string, 0, count.Value())
	for i := 0; i < count.Value(); i++ {
		var s internal.String
		if err := s.Read(rw); err != nil {
			return res, []string{}, errors.Wrapf(err, "failed to read name value with index "+strconv.Itoa(i))
		}
		names = append(names, s.Value())
	}

	return res, names, nil
}
