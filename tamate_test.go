package tamate

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/go-tamate/tamate/driver"
	"github.com/stretchr/testify/assert"
)

type fakeDriver struct{}

func (d *fakeDriver) Open(ctx context.Context, dsn string) (driver.Conn, error) {
	return nil, nil
}

func TestRegister(t *testing.T) {
	type args struct {
		name   string
		driver driver.Driver
	}
	tests := []struct {
		name      string
		args      args
		wantPanic bool
	}{
		{
			name: "ok",
			args: args{
				name:   "test",
				driver: &fakeDriver{},
			},
			wantPanic: false,
		},
		{
			name: "driver is nil",
			args: args{
				name:   "test",
				driver: nil,
			},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				unregisterAllDrivers()
				err := recover()
				if (err != nil) != tt.wantPanic {
					t.Errorf("got panic = %v, wantPanic %v", err, tt.wantPanic)
					return
				}
			}()
			Register(tt.args.name, tt.args.driver)
			assert.Contains(t, Drivers(), tt.args.name)
		})
	}
}

func TestRegister_CalledTwiceWithName(t *testing.T) {
	type args struct {
		name   string
		driver driver.Driver
	}
	tests := []struct {
		name      string
		args      args
		wantPanic bool
	}{
		{
			name: "called twice with name",
			args: args{
				name:   "test",
				driver: &fakeDriver{},
			},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				unregisterAllDrivers()
				err := recover()
				if (err != nil) != tt.wantPanic {
					t.Errorf("got panic = %v, wantPanic %v", err, tt.wantPanic)
					return
				}
			}()
			Register(tt.args.name, tt.args.driver)
			Register(tt.args.name, tt.args.driver)
			assert.Contains(t, Drivers(), tt.args.name)
		})
	}
}

func TestRegister_Goroutine(t *testing.T) {
	const total = 1000

	type args struct {
		driver driver.Driver
	}
	tests := []struct {
		name      string
		args      args
		wantPanic bool
	}{
		{
			name: "goroutine",
			args: args{
				driver: &fakeDriver{},
			},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				unregisterAllDrivers()
				err := recover()
				if (err != nil) != tt.wantPanic {
					t.Errorf("got panic = %v, wantPanic %v", err, tt.wantPanic)
					return
				}
			}()
			wg := &sync.WaitGroup{}
			for i := 0; i < total; i++ {
				wg.Add(1)
				name := strconv.Itoa(i)
				go func() {
					Register(name, tt.args.driver)
					wg.Done()
				}()
			}
			wg.Wait()
			assert.Equal(t, len(Drivers()), total)
		})
	}
}

func TestDrivers(t *testing.T) {
	const (
		name1 = "driver1"
		name2 = "driver2"
	)
	Register(name1, &fakeDriver{})
	Register(name2, &fakeDriver{})

	tests := []struct {
		name string
		want []string
	}{
		{
			name: "ok",
			want: []string{
				name1,
				name2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Drivers(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Drivers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDrivers_Goroutine(t *testing.T) {
	const total = 1000

	tests := []struct {
		name string
	}{
		{
			name: "ok goroutine",
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			wg := &sync.WaitGroup{}
			for i := 0; i < total; i++ {
				wg.Add(1)
				index := i
				Register(strconv.Itoa(index), &fakeDriver{})
				go func() {
					Drivers()
					wg.Done()
				}()
			}
			wg.Wait()
		})
	}
}

type fakeRows struct {
	current        int
	fakeColumns    []*driver.Column
	fakeRowsValues [][]driver.Value
}

func (f *fakeRows) Columns() []*driver.Column {
	return f.fakeColumns
}

func (f *fakeRows) Close() error {
	return nil
}

func (f *fakeRows) Next(dest []driver.Value) error {
	if f.current >= len(f.fakeRowsValues) {
		return io.EOF
	}
	for i := range dest {
		dest[i] = f.fakeRowsValues[f.current][i]
	}
	f.current++
	return nil
}

func (f *fakeRows) HasNextResultSet() bool {
	return false
}

func (f *fakeRows) NextResultSet() error {
	return nil
}

type notExpandRows struct{}

func (f *notExpandRows) Columns() []*driver.Column { return []*driver.Column{} }

func (f *notExpandRows) Close() error { return nil }

func (f *notExpandRows) Next(dest []driver.Value) error { return io.EOF }

type errorRows struct{}

func (f *errorRows) Columns() []*driver.Column { return []*driver.Column{} }

func (f *errorRows) Close() error { return errors.New("same error") }

func (f *errorRows) Next(dest []driver.Value) error { return errors.New("same error") }

func TestRows_Next(t *testing.T) {
	type fields struct {
		rowsi    driver.Rows
		lastcols []driver.Value
		closed   bool
		lasterr  error
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "successfully",
			fields: fields{
				rowsi: &fakeRows{
					fakeRowsValues: [][]driver.Value{[]driver.Value{}},
				},
			},
			want: true,
		},
		{
			name: "already closed",
			fields: fields{
				closed: true,
			},
			want: false,
		},
		{
			name: "lastcol is nil",
			fields: fields{
				rowsi: &fakeRows{},
			},
			want: false,
		},
		{
			name: "not expand rows",
			fields: fields{
				rowsi: &notExpandRows{},
			},
			want: false,
		},
		{
			name: "Next() returns same error",
			fields: fields{
				rowsi: &errorRows{},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &Rows{
				rowsi:    tt.fields.rowsi,
				lastcols: tt.fields.lastcols,
				closed:   tt.fields.closed,
				lasterr:  tt.fields.lasterr,
			}
			if got := rs.Next(); got != tt.want {
				t.Errorf("Rows.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRows_Next_Goroutine(t *testing.T) {
	const total = 1000

	type fields struct {
		rowsi    driver.Rows
		lastcols []driver.Value
		closed   bool
		lasterr  error
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "ok goroutine",
			fields: fields{
				rowsi: &fakeRows{},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wg := &sync.WaitGroup{}
			for i := 0; i < total; i++ {
				wg.Add(1)
				go func() {
					rs := &Rows{
						rowsi:    tt.fields.rowsi,
						lastcols: tt.fields.lastcols,
						closed:   tt.fields.closed,
						lasterr:  tt.fields.lasterr,
					}
					if got := rs.Next(); got != tt.want {
						t.Errorf("Rows.Next() = %v, want %v", got, tt.want)
					}
					wg.Done()
				}()
			}
			wg.Wait()
		})
	}
}

func TestRows_Close(t *testing.T) {
	type fields struct {
		rowsi    driver.Rows
		lastcols []driver.Value
		closed   bool
		lasterr  error
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "successfully",
			fields: fields{
				rowsi: &fakeRows{},
			},
			wantErr: false,
		},
		{
			name: "already closed",
			fields: fields{
				closed: true,
			},
			wantErr: false,
		},
		{
			name: "Close() returns same error",
			fields: fields{
				rowsi: &errorRows{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &Rows{
				rowsi:    tt.fields.rowsi,
				lastcols: tt.fields.lastcols,
				closed:   tt.fields.closed,
				lasterr:  tt.fields.lasterr,
			}
			if err := rs.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Rows.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRows_Close_Goroutine(t *testing.T) {
	const total = 1000

	type fields struct {
		rowsi    driver.Rows
		lastcols []driver.Value
		closed   bool
		lasterr  error
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "ok goroutine",
			fields: fields{
				rowsi: &fakeRows{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wg := &sync.WaitGroup{}
			for i := 0; i < total; i++ {
				wg.Add(1)
				go func() {
					rs := &Rows{
						rowsi:    tt.fields.rowsi,
						lastcols: tt.fields.lastcols,
						closed:   tt.fields.closed,
						lasterr:  tt.fields.lasterr,
					}
					if err := rs.Close(); (err != nil) != tt.wantErr {
						t.Errorf("Rows.Close() error = %v, wantErr %v", err, tt.wantErr)
					}
					wg.Done()
				}()
			}
			wg.Wait()
		})
	}
}
