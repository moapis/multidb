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

func multiTestConnect() (*MultiDB, []sm.Sqlmock, error) {
	var (
		mocks []sm.Sqlmock
		nodes []*Node
	)
	for i := 0; i < 3; i++ {
		db, mock, err := sm.New()
		if err != nil {
			return nil, nil, err
		}
		mocks = append(mocks, mock)
		nodes = append(nodes, &Node{
			db: db,
			nodeStats: nodeStats{
				maxFails: -1,
			},
		})
	}
	return &MultiDB{all: nodes}, mocks, nil
}

func Test_multiExec(t *testing.T) {
	t.Log("All nodes healthy")
	mdb, mocks, err := multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
	}
	got, err := multiExec(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err := got.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("multiExec() Res = %v, want %v", i, 3)
	}

	t.Log("Healty delayed, two error")
	mdb, mocks, err = multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectExec(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnResult(sm.NewResult(2, 3))
		} else {
			mock.ExpectExec(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}
	got, err = multiExec(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	i, err = got.RowsAffected()
	if err != nil || i != 3 {
		t.Errorf("multiExec() Res = %v, want %v", i, 3)
	}

	t.Log("All same error")
	mdb, mocks, err = multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectExec(testQuery).WillReturnError(sql.ErrNoRows)
	}
	got, err = multiExec(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	if err != sql.ErrNoRows {
		t.Errorf("exec() expected err: %v, got: %v", sql.ErrNoRows, err)
	}
	if got != nil {
		t.Errorf("multiExec() Res = %v, want %v", got, nil)
	}

	t.Log("Different errors")
	mdb, mocks, err = multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectExec(testQuery).WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectExec(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}
	got, err = multiExec(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	if err == nil {
		t.Errorf("multiExec() expected err got: %v", err)
	}
	_, ok := err.(MultiError)
	if !ok {
		t.Errorf("multiExec() expected err type: %T, got: %T", MultiError{}, err)
	}
	if got != nil {
		t.Errorf("multiExec() Res = %v, want %v", got, nil)
	}
}

func Test_multiQuery(t *testing.T) {
	t.Log("All nodes healthy")
	mdb, mocks, err := multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	want := "value"
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
	}
	rows, err := multiQuery(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	rows.Next()
	var got string
	if err = rows.Scan(&got); err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("multiQuery() R = %v, want %v", got, want)
	}

	t.Log("Healty delayed, two error")
	mdb, mocks, err = multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectQuery(testQuery).WillDelayFor(1 * time.Second).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
		} else {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}
	rows, err = multiQuery(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	if err != nil {
		t.Error(err)
	}
	rows.Next()
	got = ""
	if err = rows.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("multiQuery() R = %v, want %v", got, want)
	}

	t.Log("All same error")
	mdb, mocks, err = multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
	}
	rows, err = multiQuery(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	if err != sql.ErrNoRows {
		t.Errorf("Expected err: %v, got: %v", sql.ErrNoRows, err)
	}
	if rows != nil {
		t.Errorf("multiQuery() Res = %v, want %v", rows, nil)
	}

	t.Log("Different errors")
	mdb, mocks, err = multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	for i, mock := range mocks {
		if i == 0 {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectQuery(testQuery).WillReturnError(sql.ErrConnDone)
		}
	}
	rows, err = multiQuery(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	if err == nil {
		t.Errorf("multiQuery() expected err got: %v", err)
	}
	_, ok := err.(MultiError)
	if !ok {
		t.Errorf("multiQuery() expected err type: %T, got: %T", MultiError{}, err)
	}
	if rows != nil {
		t.Errorf("multiQuery() Res = %v, want %v", rows, nil)
	}
}

func Test_multiQueryRow(t *testing.T) {
	t.Log("All nodes healthy")
	mdb, mocks, err := multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	want := "value"
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WithArgs(1).WillReturnRows(sm.NewRows([]string{"some"}).AddRow(want))
	}
	row := multiQueryRow(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	var got string
	if err = row.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("multiQueryRow() R = %v, want %v", got, want)
	}

	t.Log("All same error")
	mdb, mocks, err = multiTestConnect()
	if err != nil {
		t.Fatal(err)
	}
	for _, mock := range mocks {
		mock.ExpectQuery(testQuery).WillReturnError(sql.ErrNoRows)
	}
	row = multiQueryRow(context.Background(), nodes2Exec(mdb.All()), testQuery, 1)
	got = ""
	err = row.Scan(&got)
	if err != sql.ErrNoRows {
		t.Errorf("Expected err: %v, got: %v", sql.ErrNoRows, err)
	}
	if got != "" {
		t.Errorf("multiQueryRow() Res = %v, want %v", got, "")
	}
}
