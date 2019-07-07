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

func (d *fakeDriver) Open(ctx context.Context, dsn string) (driver.DriverContext, error) {
	if dsn == "test" {
		return &fakeDriverContext{}, nil
	}
	return nil, errors.New("same error")
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
	fakeColumns    []string
	fakeRowsValues [][]*driver.NamedValue
}

func (f *fakeRows) Columns() []string {
	return f.fakeColumns
}

func (f *fakeRows) Close() error {
	return nil
}

func (f *fakeRows) Next(dest []*driver.NamedValue) error {
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

func (f *notExpandRows) Columns() []string { return []string{} }

func (f *notExpandRows) Close() error { return nil }

func (f *notExpandRows) Next(dest []*driver.NamedValue) error { return io.EOF }

type errorRows struct{}

func (f *errorRows) Columns() []string { return []string{} }

func (f *errorRows) Close() error { return errors.New("same error") }

func (f *errorRows) Next(dest []*driver.NamedValue) error { return errors.New("same error") }

func TestRows_Next(t *testing.T) {
	type fields struct {
		rowsi    driver.Rows
		lastcols []*driver.NamedValue
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
					fakeRowsValues: [][]*driver.NamedValue{[]*driver.NamedValue{}},
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
		lastcols []*driver.NamedValue
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
		lastcols []*driver.NamedValue
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
		lastcols []*driver.NamedValue
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

func TestRows_Scan(t *testing.T) {
	type fields struct {
		rowsi    driver.Rows
		lastcols []*driver.NamedValue
		closed   bool
		lasterr  error
	}
	type args struct {
		dest []*driver.NamedValue
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "ok",
			fields: fields{
				rowsi: &fakeRows{
					fakeColumns: []string{"a"},
				},
				lastcols: []*driver.NamedValue{
					&driver.NamedValue{
						Name:  "a",
						Value: 1,
					},
				},
				closed:  false,
				lasterr: nil,
			},
			args:    args{},
			wantErr: false,
		},
		{
			name: "has error",
			fields: fields{
				rowsi:    &fakeRows{},
				lastcols: []*driver.NamedValue{},
				closed:   false,
				lasterr:  errors.New("same errorw"),
			},
			args:    args{},
			wantErr: true,
		},
		{
			name: "already closed",
			fields: fields{
				rowsi:    &fakeRows{},
				lastcols: []*driver.NamedValue{},
				closed:   true,
				lasterr:  nil,
			},
			args:    args{},
			wantErr: true,
		},
		{
			name: "not called Next()",
			fields: fields{
				rowsi:    &fakeRows{},
				lastcols: nil,
				closed:   false,
				lasterr:  nil,
			},
			args:    args{},
			wantErr: true,
		},
		{
			name: "not equal length of lastcol and columns",
			fields: fields{
				rowsi: &fakeRows{
					fakeColumns: []string{"col1"},
				},
				lastcols: []*driver.NamedValue{},
				closed:   false,
				lasterr:  nil,
			},
			args:    args{},
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
			if err := rs.Scan(tt.args.dest); (err != nil) != tt.wantErr {
				t.Errorf("Rows.Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type fakeSchema struct{}

type fakeDriverContext struct{}

func (f *fakeDriverContext) GetSchema(ctx context.Context, tableName string) (driver.Schema, error) {
	return &fakeSchema{}, nil
}

func (f *fakeDriverContext) GetRows(context.Context, string) (driver.Rows, error) {
	return &fakeRows{}, nil
}

func (f *fakeDriverContext) Close() error {
	return nil
}

func TestOpenDataSource(t *testing.T) {
	type args struct {
		ctx driver.DriverContext
	}
	tests := []struct {
		name string
		args args
		want *DataSource
	}{
		{
			name: "ok",
			args: args{
				ctx: &fakeDriverContext{},
			},
			want: &DataSource{
				ctx:    &fakeDriverContext{},
				closed: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := OpenDataSource(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OpenDataSource() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataSource_GetSchema(t *testing.T) {
	type fields struct {
		ctx    driver.DriverContext
		closed bool
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    driver.Schema
		wantErr bool
	}{
		{
			name: "ok",
			fields: fields{
				ctx:    &fakeDriverContext{},
				closed: false,
			},
			args: args{
				ctx:  context.Background(),
				name: "",
			},
			want:    &fakeSchema{},
			wantErr: false,
		},
		{
			name: "already closed",
			fields: fields{
				ctx:    &fakeDriverContext{},
				closed: true,
			},
			args: args{
				ctx:  context.Background(),
				name: "",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &DataSource{
				ctx:    tt.fields.ctx,
				closed: tt.fields.closed,
			}
			got, err := ds.GetSchema(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataSource.GetSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataSource.GetSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataSource_GetRows(t *testing.T) {
	type fields struct {
		ctx    driver.DriverContext
		closed bool
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    driver.Rows
		wantErr bool
	}{
		{
			name: "ok",
			fields: fields{
				ctx:    &fakeDriverContext{},
				closed: false,
			},
			args: args{
				ctx:  context.Background(),
				name: "",
			},
			want:    &fakeRows{},
			wantErr: false,
		},
		{
			name: "already closed",
			fields: fields{
				ctx:    &fakeDriverContext{},
				closed: true,
			},
			args: args{
				ctx:  context.Background(),
				name: "",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &DataSource{
				ctx:    tt.fields.ctx,
				closed: tt.fields.closed,
			}
			got, err := ds.GetRows(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataSource.GetRows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataSource.GetRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataSource_Close(t *testing.T) {
	type fields struct {
		ctx    driver.DriverContext
		closed bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "ok",
			fields: fields{
				ctx:    &fakeDriverContext{},
				closed: false,
			},
			wantErr: false,
		},
		{
			name: "already closed",
			fields: fields{
				ctx:    &fakeDriverContext{},
				closed: true,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &DataSource{
				ctx:    tt.fields.ctx,
				closed: tt.fields.closed,
			}
			if err := ds.Close(); (err != nil) != tt.wantErr {
				t.Errorf("DataSource.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOpen(t *testing.T) {
	const driverName = "test"
	Register(driverName, &fakeDriver{})

	type args struct {
		driverName     string
		dataSourceName string
	}
	tests := []struct {
		name    string
		args    args
		want    *DataSource
		wantErr bool
	}{
		{
			name: "ok",
			args: args{
				driverName:     driverName,
				dataSourceName: "test",
			},
			want: &DataSource{
				ctx:    &fakeDriverContext{},
				closed: false,
			},
			wantErr: false,
		},
		{
			name: "driver open error",
			args: args{
				driverName:     driverName,
				dataSourceName: "same error",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "open not register driver",
			args: args{
				driverName: "not register",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Open(tt.args.driverName, tt.args.dataSourceName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Open() = %v, want %v", got, tt.want)
			}
		})
	}
}
