// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package multidb

import (
	"database/sql"
	"math"
	"reflect"
	"testing"
)

func TestNode(t *testing.T) {
	want := "foo"
	node := NewNode(want, nil)
	got := node.Name()

	if got != want {
		t.Errorf("Node.Name() = %s, want %s", got, want)
	}
}

func Test_useFactor(t *testing.T) {
	stats := new(sql.DB).Stats()

	tests := []struct {
		name  string
		stats *sql.DBStats
		want  float64
	}{
		{
			"empty DB",
			&stats,
			0,
		},
		{
			"NaN",
			&sql.DBStats{
				MaxOpenConnections: 0,
				InUse:              0,
			},
			0,
		},
		{
			"Inf",
			&sql.DBStats{
				MaxOpenConnections: 0,
				InUse:              1,
			},
			math.Inf(1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := useFactor(tt.stats); got != tt.want {
				t.Errorf("useFactor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readOnlyOpts(t *testing.T) {
	tests := []struct {
		name string
		opts *sql.TxOptions
		want *sql.TxOptions
	}{
		{
			"Nil opts",
			nil,
			&sql.TxOptions{ReadOnly: true},
		},
		{
			"ReadOnly true",
			&sql.TxOptions{ReadOnly: true},
			&sql.TxOptions{ReadOnly: true},
		},
		{
			"Isolation level",
			&sql.TxOptions{Isolation: sql.LevelLinearizable},
			&sql.TxOptions{Isolation: sql.LevelLinearizable, ReadOnly: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readOnlyOpts(tt.opts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readOnlyOpts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nodeList(t *testing.T) {
	list := nodeList{
		&Node{"one", &sql.DB{}},
		&Node{"two", &sql.DB{}},
		&Node{"three", &sql.DB{}},
	}

	if ln := list.Len(); ln != len(list) {
		t.Errorf("nodeList.Len = %v, want %v", ln, len(list))
	}

	if less := list.Less(0, 1); less {
		t.Errorf("nodeList.Less = %v, want %v", less, false)
	}

	list.Swap(0, 1)

	if list[0].name != "two" || list[1].name != "one" || list[2].name != "three" {
		t.Errorf("nodeList.Swap order wrong: %v", list)
	}
}
