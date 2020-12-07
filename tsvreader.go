package clickhouse

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

type tsvReader struct {
	row       int
	col       int
	maxCol    int
	readBuf   [16 * 1024]byte // fixed read buffer
	readSlice []byte          // read slice
	rowBytes  []byte          // row buffer
	rowSlice  []byte          // row slice
	r         io.Reader
	err       error
}

func (r *tsvReader) Error() error {
	return r.err
}

func (r *tsvReader) HasCols() bool {
	if r.err != nil {
		return false
	}
	if r.row == 0 {
		r.err = errors.New("NextRow must be called before")
		return false
	}
	return r.rowSlice != nil
}

func (r *tsvReader) NextRow() bool {
	if r.err != nil {
		return false
	}

	if r.maxCol < r.col {
		r.maxCol = r.col
	}

	r.row++
	r.col = 0
	r.rowBytes = r.rowBytes[:0]
	r.rowSlice = nil

	for {
		if p := bytes.IndexByte(r.readSlice, '\n'); p >= 0 { // end of the row is found in read buffer
			if r.maxCol > 1 && p == 0 { // skip empty line if there are more than one column
				r.readSlice = r.readSlice[1:]
				continue
			}
			r.rowBytes = append(r.rowBytes, r.readSlice[0:p]...) // append data till newline
			r.rowSlice = r.rowBytes
			r.readSlice = r.readSlice[p+1:] // update read buffer slice
			return true
		}

		var offset = copy(r.readBuf[:], r.readSlice)
		n, err := r.r.Read(r.readBuf[offset:])
		r.readSlice = r.readBuf[0 : offset+n]

		if err == io.EOF {
			if n == 0 {
				if len(r.readSlice) > 0 {
					r.err = fmt.Errorf("can not find newline for row #%d", r.row)
				} else {
					r.err = io.EOF
				}
				return false
			}
		} else if err != nil {
			r.err = fmt.Errorf("failed to read row #%d: %w", r.row, err)
			return false
		}
	}
}

func (r *tsvReader) Bytes() []byte {
	if r.err != nil || r.rowSlice == nil {
		return nil
	}
	r.col++
	if p := bytes.IndexByte(r.rowSlice, '\t'); p >= 0 {
		var b = r.rowSlice[0:p]
		r.rowSlice = r.rowSlice[p+1:]
		return b
	}
	// last column
	var b = r.rowSlice
	r.rowSlice = nil
	return b
}

func (r *tsvReader) String() string {
	return string(r.Bytes())
}
