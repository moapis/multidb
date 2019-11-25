package multidb

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	sm "github.com/DATA-DOG/go-sqlmock"
)

func TestMultiError_Error(t *testing.T) {
	tests := []struct {
		name   string
		errors []error
		want   string
	}{
		{
			"No cases",
			nil,
			"Unknown error",
		},
		{
			"Mutiple cases",
			[]error{
				errors.New("First"),
				errors.New("Second"),
				errors.New("Third"),
			},
			"Multiple errors: 1: First; 2: Second; 3: Third;",
		},
		{
			"With nil",
			[]error{
				nil,
				errors.New("First"),
				errors.New("Second"),
				errors.New("Third"),
			},
			"Multiple errors: 1: <nil>; 2: First; 3: Second; 4: Third;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := MultiError{
				Errors: tt.errors,
			}
			if got := me.Error(); got != tt.want {
				t.Errorf("MultiError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiError_append(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name   string
		errors []error
		args   error
		want   []error
	}{
		{
			"Append",
			[]error{
				errors.New("First"),
				errors.New("Second"),
			},
			errors.New("Third"),
			[]error{
				errors.New("First"),
				errors.New("Second"),
				errors.New("Third"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := &MultiError{
				Errors: tt.errors,
			}
			me.append(tt.args)
			if !reflect.DeepEqual(tt.want, me.Errors) {
				t.Errorf("MultiError.append() = %v, want %v", me.Errors, tt.want)
			}
		})
	}
}

func TestMultiError_check(t *testing.T) {
	tests := []struct {
		name   string
		errors []error
		want   error
	}{
		{
			"No cases",
			nil,
			nil,
		},
		{
			"Different cases",
			[]error{
				errors.New("First"),
				errors.New("Second"),
				errors.New("Third"),
			},
			MultiError{
				[]error{
					errors.New("First"),
					errors.New("Second"),
					errors.New("Third"),
				},
			},
		},
		{
			"With nil",
			[]error{
				nil,
				errors.New("First"),
				errors.New("Second"),
				errors.New("Third"),
			},
			MultiError{
				[]error{
					nil,
					errors.New("First"),
					errors.New("Second"),
					errors.New("Third"),
				},
			},
		},
		{
			"Same cases",
			[]error{
				sql.ErrNoRows,
				sql.ErrNoRows,
				sql.ErrNoRows,
			},
			sql.ErrNoRows,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := MultiError{
				Errors: tt.errors,
			}
			if got := me.check(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MultiError.check() error = %v, want %v", got, tt.want)
			}
		})
	}
}

func multiTestConnect(dsns []string) (*MultiDB, []sm.Sqlmock, error) {
	conf := Config{
		DBConf: testConfig{
			dn:   testDBDriver,
			dsns: dsns,
		},
		StatsLen:      100,
		FailPrecent:   10,
		ReconnectWait: 0,
	}
	var mocks []sm.Sqlmock
	for _, dns := range dsns {
		_, mock, err := sm.NewWithDSN(dns)
		if err != nil {
			return nil, nil, err
		}
		mocks = append(mocks, mock)
	}
	mdb, err := conf.Open()
	if err != nil {
		return nil, nil, err
	}
	return mdb, mocks, err
}

func Test_multiExec(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"g", "h", "i"})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("All nodes healthy", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
		}
		got, err := multiExec(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		if err != nil {
			t.Error(err)
		}
		i, err := got.RowsAffected()
		if err != nil || i != 3 {
			t.Errorf("exec() Res = %v, want %v", i, 3)
		}
	})
	t.Run("Healty delayed, two error", func(t *testing.T) {
		for i, mock := range mocks {
			if i == 0 {
				mock.ExpectExec(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
			} else {
				mock.ExpectExec(testQuery).WillReturnError(sql.ErrConnDone)
			}
		}
		got, err := multiExec(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		if err != nil {
			t.Error(err)
		}
		i, err := got.RowsAffected()
		if err != nil || i != 3 {
			t.Errorf("exec() Res = %v, want %v", i, 3)
		}
	})
	t.Run("All same error", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectExec(testQuery).WillReturnError(sql.ErrNoRows)
		}
		got, err := multiExec(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		if err != sql.ErrNoRows {
			t.Errorf("exec() expected err: %v, got: %v", sql.ErrNoRows, err)
		}
		if got != nil {
			t.Errorf("exec() Res = %v, want %v", got, nil)
		}
	})
	t.Run("Different errors", func(t *testing.T) {
		for i, mock := range mocks {
			if i == 0 {
				mock.ExpectExec(testQuery).WillReturnError(sql.ErrNoRows)
			} else {
				mock.ExpectExec(testQuery).WillReturnError(sql.ErrConnDone)
			}
		}
		got, err := multiExec(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		if err == nil {
			t.Errorf("exec() expected err got: %v", err)
		}
		_, ok := err.(MultiError)
		if !ok {
			t.Errorf("exec() expected err type: %T, got: %T", MultiError{}, err)
		}
		if got != nil {
			t.Errorf("exec() Res = %v, want %v", got, nil)
		}
	})
	t.Run("Expire context", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectExec(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		got, err := multiExec(ctx, nodes2Exec(mdb.All()), testQuery, 1)
		if err != context.DeadlineExceeded {
			t.Errorf("exec() expected err: %v, got: %v", context.DeadlineExceeded, err)
		}
		if got != nil {
			t.Errorf("exec() Res = %v, want %v", got, nil)
		}
	})
}

func Test_multiQuery(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"j", "k", "l"})
	if err != nil {
		t.Fatal(err)
	}
	want := "value"
	t.Run("All nodes healthy", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		}
		r, err := multiQuery(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		if err != nil {
			t.Error(err)
		}
		r.Next()
		var got string
		if err = r.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("query() R = %v, want %v", got, want)
		}
	})
	t.Run("Healty delayed, two error", func(t *testing.T) {
		for i, mock := range mocks {
			if i == 0 {
				mock.ExpectQuery(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
			} else {
				mock.ExpectQuery(testQuery).WillReturnError(sql.ErrConnDone)
			}
		}
		r, err := multiQuery(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		if err != nil {
			t.Error(err)
		}
		r.Next()
		var got string
		if err = r.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("query() R = %v, want %v", got, want)
		}
	})
	t.Run("All same error", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
		}
		got, err := multiQuery(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		if err != sql.ErrNoRows {
			t.Errorf("Expected err: %v, got: %v", sql.ErrNoRows, err)
		}
		if got != nil {
			t.Errorf("query() Res = %v, want %v", got, nil)
		}
	})
	t.Run("Different errors", func(t *testing.T) {
		for i, mock := range mocks {
			if i == 0 {
				mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
			} else {
				mock.ExpectQuery(testQuery).WillReturnError(sql.ErrConnDone)
			}
		}
		got, err := multiQuery(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		if err == nil {
			t.Errorf("query() expected err got: %v", err)
		}
		_, ok := err.(MultiError)
		if !ok {
			t.Errorf("query() expected err type: %T, got: %T", MultiError{}, err)
		}
		if got != nil {
			t.Errorf("query() Res = %v, want %v", got, nil)
		}
	})
	t.Run("Expire context", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		got, err := multiQuery(ctx, nodes2Exec(mdb.All()), testQuery, 1)
		if err != context.DeadlineExceeded {
			t.Errorf("query() expected err: %v, got: %v", context.DeadlineExceeded, err)
		}
		if got != nil {
			t.Errorf("query() Res = %v, want %v", got, nil)
		}
	})
}

func Test_multiQueryRow(t *testing.T) {
	mdb, mocks, err := multiTestConnect([]string{"m", "n", "o"})
	if err != nil {
		t.Fatal(err)
	}
	want := "value"
	t.Run("All nodes healthy", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		}
		r := multiQueryRow(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		var got string
		if err = r.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("query() R = %v, want %v", got, want)
		}
	})
	time.Sleep(10 * time.Millisecond)
	t.Run("All same error", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
		}
		row := multiQueryRow(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
		var got string
		err := row.Scan(&got)
		if err != sql.ErrNoRows {
			t.Errorf("Expected err: %v, got: %v", sql.ErrNoRows, err)
		}
		if got != "" {
			t.Errorf("query() Res = %v, want %v", got, nil)
		}
	})
	time.Sleep(10 * time.Millisecond)
	t.Run("Expire context", func(t *testing.T) {
		for _, mock := range mocks {
			mock.ExpectQuery(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		row := multiQueryRow(ctx, nodes2Exec(mdb.All()), testQuery, 1)
		var got string
		err := row.Scan(&got)
		if err == nil || err.Error() != "canceling query due to user request" {
			t.Errorf("Expected err: %v, got: %v", context.DeadlineExceeded, err)
		}
		if got != "" {
			t.Errorf("query() Res = %v, want %v", got, nil)
		}
	})
}
