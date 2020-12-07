package clickhouse

import (
	"errors"
	assert "github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
)

func TestTsvReader(t *testing.T) {
	var readAll = func(t *testing.T, r *tsvReader) [][]string {
		var res [][]string
		for r.NextRow() {
			var row []string
			for r.HasCols() {
				row = append(row, r.String())
			}
			res = append(res, row)
		}
		return res
	}

	t.Run("error: NextRow not called", func(t *testing.T) {
		var r = &tsvReader{r: strings.NewReader(`a`)}
		assert.NoError(t, r.Error())

		assert.Nil(t, r.Bytes())
		assert.NoError(t, r.Error())

		assert.Exactly(t, ``, r.String())
		assert.NoError(t, r.Error())

		assert.False(t, r.HasCols())
		assert.EqualError(t, r.Error(), `NextRow must be called before`)
	})

	t.Run("error: last line without newline in the end", func(t *testing.T) {
		var r = &tsvReader{r: strings.NewReader("a\tb\nc\td\ne\tf")}
		assert.Exactly(t, [][]string{{"a", "b"}, {"c", "d"}}, readAll(t, r))
		assert.EqualError(t, r.Error(), `can not find newline for row #3`)
	})

	t.Run("3 rows * 2 cols", func(t *testing.T) {
		var r = &tsvReader{r: strings.NewReader("a\tb\nc\td\ne\tf\n")}
		assert.Exactly(t, [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}}, readAll(t, r))
		assert.Exactly(t, io.EOF, r.Error())
	})

	t.Run("variadic number of columns", func(t *testing.T) {
		var r = &tsvReader{r: strings.NewReader("a\tb\tc\nd\te\nf\n")}
		assert.Exactly(t, [][]string{{"a", "b", "c"}, {"d", "e"}, {"f"}}, readAll(t, r))
		assert.Exactly(t, io.EOF, r.Error())
	})

	t.Run("empty lines are valid for single column", func(t *testing.T) {
		var r = &tsvReader{r: strings.NewReader("a\nb\nc\n\n\nd\n\n")}
		assert.Exactly(t, [][]string{{"a"}, {"b"}, {"c"}, {""}, {""}, {"d"}, {""}}, readAll(t, r))
		assert.Exactly(t, io.EOF, r.Error())
	})

	t.Run("empty lines are skipped for multiple columns", func(t *testing.T) {
		var r = &tsvReader{r: strings.NewReader("a\tb\nc\td\n\n\ne\tf\n\n")}
		assert.Exactly(t, [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}}, readAll(t, r))
		assert.Exactly(t, io.EOF, r.Error())
	})

	t.Run("no unescape (raw data)", func(t *testing.T) {
		var r = &tsvReader{r: strings.NewReader("a\t\\'b\n'c\td\n\"\\\\e\tf\n\n")}
		assert.Exactly(t, [][]string{{`a`, `\'b`}, {`'c`, `d`}, {`"\\e`, `f`}}, readAll(t, r))
		assert.Exactly(t, io.EOF, r.Error())
	})

	t.Run("read buffer issue", func(t *testing.T) {
		var r = &tsvReader{r: &chunkedReader{Chunks: []string{"a\tb\nc\td", "\ne\tf\n"}}}
		assert.Exactly(t, [][]string{{`a`, `b`}, {`c`, `d`}, {`e`, `f`}}, readAll(t, r))
		assert.Exactly(t, io.EOF, r.Error())
	})
}

var _ io.Reader = (*chunkedReader)(nil)

type chunkedReader struct {
	Chunks    []string
	nextChunk int
}

func (r *chunkedReader) Read(p []byte) (n int, err error) {
	if r.nextChunk >= len(r.Chunks) {
		return 0, io.EOF
	}
	var chunk = []byte(r.Chunks[r.nextChunk])
	if len(p) < len(chunk) {
		return 0, errors.New("chunk length is greater than read buffer length")
	}
	r.nextChunk++
	copy(p[0:len(chunk)], chunk)
	return len(chunk), nil
}
