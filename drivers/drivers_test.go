// Copyright (c) 2020, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package drivers

import (
	"context"
	"database/sql"
	"testing"

	sm "github.com/DATA-DOG/go-sqlmock"
)

const testMasterQuery = "select ismaster"

func TestMasterFunc(t *testing.T) {
	db, mock, err := sm.New()
	if err != nil {
		t.Fatal(err)
	}

	mf := MasterFunc(testMasterQuery)

	mock.ExpectQuery(testMasterQuery).
		WillReturnRows(sm.NewRows([]string{"ismaster"}).AddRow(true))

	b, err := mf(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	if !b {
		t.Errorf("MasterFunc = %v, want %v", b, true)
	}

	mock.ExpectQuery(testMasterQuery).
		WillReturnRows(sm.NewRows([]string{"ismaster"}).AddRow(false))

	b, err = mf(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	if b {
		t.Errorf("MasterFunc = %v, want %v", b, false)
	}

	mock.ExpectQuery(testMasterQuery).WillReturnError(sql.ErrNoRows)

	b, err = mf(context.Background(), db)
	if err != sql.ErrNoRows {
		t.Fatalf("MasterFunc err = %v, want %v", err, sql.ErrNoRows)
	}

	if b {
		t.Errorf("MasterFunc = %v, want %v", b, true)
	}
}
