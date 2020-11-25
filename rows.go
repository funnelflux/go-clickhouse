package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

func newTextRows(c *conn, body io.ReadCloser, location *time.Location, useDBLocation bool) (*textRows, error) {
	tsv := tsvReader{r: body}

	var columns []string
	if tsv.NextRow() {
		for tsv.HasCols() {
			columns = append(columns, tsv.String())
		}
	}

	var types []string
	if tsv.NextRow() {
		for tsv.HasCols() {
			if v, err := readUnquoted(strings.NewReader(tsv.String()), 0); err != nil {
				return nil, err
			} else {
				types = append(types, v)
			}
		}
	}

	if err := tsv.Error(); err != nil {
		return nil, err
	}

	parsers := make([]DataParser, len(types), len(types))
	for i, typ := range types {
		desc, err := ParseTypeDesc(typ)
		if err != nil {
			return nil, err
		}

		parsers[i], err = NewDataParser(desc, &DataParserOptions{
			Location:      location,
			UseDBLocation: useDBLocation,
		})
		if err != nil {
			return nil, err
		}
	}

	return &textRows{
		c:        c,
		respBody: body,
		tsv:      tsv,
		columns:  columns,
		types:    types,
		parsers:  parsers,
	}, nil
}

type textRows struct {
	c        *conn
	respBody io.ReadCloser
	tsv      tsvReader
	columns  []string
	types    []string
	parsers  []DataParser
}

func (r *textRows) Columns() []string {
	return r.columns
}

func (r *textRows) Close() error {
	r.c.cancel = nil
	return r.respBody.Close()
}

func (r *textRows) Next(dest []driver.Value) error {
	r.tsv.NextRow()
	if err := r.tsv.Error(); err != nil {
		return err
	}

	i := 0
	for r.tsv.HasCols() {
		reader := bytes.NewReader(r.tsv.Bytes())
		v, err := r.parsers[i].Parse(reader)
		if err != nil {
			return err
		}
		if _, _, err := reader.ReadRune(); err != io.EOF {
			return fmt.Errorf("trailing data after parsing the value")
		}
		dest[i] = v
		i++
	}

	return r.tsv.Error()
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType
func (r *textRows) ColumnTypeScanType(index int) reflect.Type {
	return r.parsers[index].Type()
}

// ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName
func (r *textRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}
