// Copyright (c) 2019, Mohlmann Solutions SRL. All rights reserved.
// Use of this source code is governed by a License that can be found in the LICENSE file.
// SPDX-License-Identifier: BSD-3-Clause

package postgresql

import (
	"reflect"
	"testing"

	"github.com/moapis/multidb/drivers"
)

// Check interface compliance
func _() drivers.Configurator { return Config{} }

func Test_connString(t *testing.T) {
	type args struct {
		Foo   string
		Bar   uint16
		Hello float32
		World int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"All zero",
			args{},
			"",
		},
		{
			"Some zero",
			args{
				Foo:   "bAr",
				Hello: 32.1,
			},
			"foo=bAr hello=32.1",
		},
		{
			"All used",
			args{
				Foo:   "bAr",
				Bar:   16,
				Hello: 32.1,
				World: 777,
			},
			"foo=bAr bar=16 hello=32.1 world=777",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := connString(tt.args); got != tt.want {
				t.Errorf("connString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_DataSourceNames(t *testing.T) {
	type fields struct {
		Nodes  []Node
		Params Params
	}
	tests := []struct {
		name     string
		fields   fields
		wantSnds []string
	}{
		{
			name: "Single host",
			fields: fields{
				Nodes: []Node{
					{
						Host: "localhost",
						Port: 22,
					},
				},
				Params: Params{
					DBname: "bar",
					User:   "world",
				},
			},
			wantSnds: []string{
				"host=localhost port=22 dbname=bar user=world",
			},
		},
		{
			name: "Multi host",
			fields: fields{
				Nodes: []Node{
					{
						Host: "mercury",
						Port: 111,
					},
					{
						Host: "venus",
						Port: 222,
					},
					{
						Host: "earth",
						Port: 333,
					},
				},
				Params: Params{
					DBname: "bar",
					User:   "world",
				},
			},
			wantSnds: []string{
				"host=mercury port=111 dbname=bar user=world",
				"host=venus port=222 dbname=bar user=world",
				"host=earth port=333 dbname=bar user=world",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				Nodes:  tt.fields.Nodes,
				Params: tt.fields.Params,
			}
			if gotSnds := c.DataSourceNames(); !reflect.DeepEqual(gotSnds, tt.wantSnds) {
				t.Errorf("Config.DataSourceNames() = %v, want %v", gotSnds, tt.wantSnds)
			}
		})
	}
}

func TestConfig_DriverName(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{
			name: "Driver name",
			want: "postgres",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c Config
			if got := c.DriverName(); got != tt.want {
				t.Errorf("Config.DriverName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_MasterQuery(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{
			"MasterQuery",
			MasterQuery,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c Config
			if got := c.MasterQuery(); got != tt.want {
				t.Errorf("Config.MasterQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
