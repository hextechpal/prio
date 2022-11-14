package core

import (
	"reflect"
	"testing"
)

func Test_calculatePartition(t *testing.T) {
	type args struct {
		topics   []string
		children []string
	}
	tests := []struct {
		name string
		args args
		want membershipData
	}{
		{
			name: "Equal Partition",
			args: args{
				topics:   []string{"t1", "t2", "t3", "t4"},
				children: []string{"c1", "c2"},
			},
			want: map[string]map[string]bool{
				"c1": {"t1": true, "t2": true},
				"c2": {"t3": true, "t4": true},
			},
		},
		{
			name: "UnEqual Partition",
			args: args{
				topics:   []string{"t1", "t2", "t3", "t4"},
				children: []string{"c1", "c2", "c3"},
			},
			want: map[string]map[string]bool{
				"c1": {"t1": true},
				"c2": {"t2": true},
				"c3": {"t3": true, "t4": true},
			},
		},
		{
			name: "More workers",
			args: args{
				topics:   []string{"t1", "t2"},
				children: []string{"c1", "c2", "c3"},
			},
			want: map[string]map[string]bool{
				"c1": {"t1": true},
				"c2": {"t2": true},
				"c3": {},
			},
		},
		{
			name: "No workers",
			args: args{
				topics:   []string{"t1", "t2"},
				children: []string{},
			},
			want: map[string]map[string]bool{},
		},
		{
			name: "No Topics",
			args: args{
				topics:   []string{},
				children: []string{"c1", "c2", "c3"},
			},
			want: map[string]map[string]bool{
				"c1": {},
				"c2": {},
				"c3": {},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculatePartition(tt.args.topics, tt.args.children)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("calculatePartition() got = %v, want %v", got, tt.want)
			}
		})
	}
}
