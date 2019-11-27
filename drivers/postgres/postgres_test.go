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
var _ = drivers.Configurator(Config{})

func Test_connString(t *testing.T) {
	type args struct {
		host   Host
		params map[string]string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Empty",
		},
		{
			name: "Host",
			args: args{
				host: Host{
					Addr: "localhost",
				},
			},
			want: "host=localhost",
		},
		{
			name: "Host, port",
			args: args{
				host: Host{
					Addr: "localhost",
					Port: 22,
				},
			},
			want: "host=localhost port=22",
		},
		{
			name: "Single param",
			args: args{
				params: map[string]string{
					"foo": "bar",
				},
			},
			want: "foo=bar",
		},
		{
			name: "Multi param",
			args: args{
				params: map[string]string{
					"foo":   "bar",
					"hello": "world",
				},
			},
			want: "foo=bar hello=world",
		},
		{
			name: "Full conf",
			args: args{
				host: Host{
					Addr: "localhost",
					Port: 22,
				},
				params: map[string]string{
					"foo":   "bar",
					"hello": "world",
				},
			},
			want: "host=localhost port=22 foo=bar hello=world",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := connString(tt.args.host, tt.args.params); got != tt.want {
				t.Errorf("connString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_DataSourceNames(t *testing.T) {
	type fields struct {
		Hosts  []Host
		Params map[string]string
	}
	tests := []struct {
		name     string
		fields   fields
		wantSnds []string
	}{
		{
			name: "Single host",
			fields: fields{
				Hosts: []Host{
					{
						Addr: "localhost",
						Port: 22,
					},
				},
				Params: map[string]string{
					"foo":   "bar",
					"hello": "world",
				},
			},
			wantSnds: []string{
				"host=localhost port=22 foo=bar hello=world",
			},
		},
		{
			name: "Multi host",
			fields: fields{
				Hosts: []Host{
					{
						Addr: "mercury",
						Port: 111,
					},
					{
						Addr: "venus",
						Port: 222,
					},
					{
						Addr: "earth",
						Port: 333,
					},
				},
				Params: map[string]string{
					"foo":   "bar",
					"hello": "world",
				},
			},
			wantSnds: []string{
				"host=mercury port=111 foo=bar hello=world",
				"host=venus port=222 foo=bar hello=world",
				"host=earth port=333 foo=bar hello=world",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				Hosts:  tt.fields.Hosts,
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
