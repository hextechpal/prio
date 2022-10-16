package worker

import (
	"testing"
)

func Test_membershipInfo_addMember(t *testing.T) {
	type fields struct {
		topics []string
		data   map[string]map[string]bool
	}
	type args struct {
		member string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		expected map[string]int
	}{
		{
			"Add Member 4 topics 2 nodes",
			fields{
				topics: []string{"topic1", "topic2", "topic3", "topic4"},
				data: map[string]map[string]bool{
					"1": {"topic1": true, "topic2": true},
					"2": {"topic3": true, "topic4": true},
				},
			},
			args{
				member: "3",
			},
			map[string]int{
				"1": 1,
				"2": 1,
				"3": 2,
			},
		},
		{
			"Add Member 8 topics 2 nodes",
			fields{
				topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7", "topic8"},
				data: map[string]map[string]bool{
					"1": {"topic1": true, "topic2": true, "topic3": true, "topic4": true},
					"2": {"topic5": true, "topic6": true, "topic7": true, "topic8": true},
				},
			},
			args{
				member: "3",
			},
			map[string]int{
				"1": 3,
				"2": 3,
				"3": 2,
			},
		},
		{
			"Add Member 8 topics 0 nodes",
			fields{
				topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7", "topic8"},
				data:   map[string]map[string]bool{},
			},
			args{
				member: "1",
			},
			map[string]int{
				"1": 8,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mi := &membershipInfo{
				topics: tt.fields.topics,
				data:   tt.fields.data,
			}
			mi.addMember(tt.args.member)
			for k, v := range tt.expected {
				if val, ok := mi.data[k]; !ok || len(val) != v {
					t.Errorf("want node %s to own %d topics, got=%v", k, v, mi.data[k])
				}
			}

		})
	}
}

func Test_membershipInfo_removeMember(t *testing.T) {
	type fields struct {
		topics []string
		data   map[string]map[string]bool
	}
	type args struct {
		member string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		expected map[string]int
	}{
		{
			"Remove Member 4 topics 3 nodes",
			fields{
				topics: []string{"topic1", "topic2", "topic3", "topic4"},
				data: map[string]map[string]bool{
					"1": {"topic1": true},
					"2": {"topic2": true},
					"3": {"topic3": true, "topic4": true},
				},
			},
			args{
				member: "3",
			},
			map[string]int{
				"1": 2,
				"2": 2,
			},
		},
		{
			"Remove Member 7 topics 3 nodes",
			fields{
				topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7"},
				data: map[string]map[string]bool{
					"1": {"topic1": true, "topic2": true},
					"2": {"topic3": true, "topic4": true},
					"3": {"topic5": true, "topic6": true, "topic7": true},
				},
			},
			args{
				member: "2",
			},
			map[string]int{
				"1": 3,
				"3": 4,
			},
		},
		{
			"Remove Member 8 topics 1 nodes",
			fields{
				topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7", "topic8"},
				data: map[string]map[string]bool{
					"1": {"topic1": true, "topic2": true, "topic3": true, "topic4": true, "topic5": true, "topic6": true, "topic7": true, "topic8": true},
				},
			},
			args{
				member: "1",
			},
			map[string]int{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mi := &membershipInfo{
				topics: tt.fields.topics,
				data:   tt.fields.data,
			}
			mi.removeMember(tt.args.member)
			for k, v := range tt.expected {
				if val, ok := mi.data[k]; !ok || len(val) != v {
					t.Errorf("want node %s to own %d topics, got=%v", k, v, mi.data[k])
				}
			}

		})
	}
}
