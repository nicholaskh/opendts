/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqltypes

import (
	"testing"

	proto2 "github.com/golang/protobuf/proto"

	"github.com/nicholaskh/opendts/proto"
)

func TestEventTokenMinimum(t *testing.T) {
	testcases := []struct {
		ev1      *proto.EventToken
		ev2      *proto.EventToken
		expected *proto.EventToken
	}{{
		ev1:      nil,
		ev2:      nil,
		expected: nil,
	}, {
		ev1: &proto.EventToken{
			Timestamp: 123,
		},
		ev2:      nil,
		expected: nil,
	}, {
		ev1: nil,
		ev2: &proto.EventToken{
			Timestamp: 123,
		},
		expected: nil,
	}, {
		ev1: &proto.EventToken{
			Timestamp: 123,
		},
		ev2: &proto.EventToken{
			Timestamp: 456,
		},
		expected: &proto.EventToken{
			Timestamp: 123,
		},
	}, {
		ev1: &proto.EventToken{
			Timestamp: 456,
		},
		ev2: &proto.EventToken{
			Timestamp: 123,
		},
		expected: &proto.EventToken{
			Timestamp: 123,
		},
	}}

	for _, tcase := range testcases {
		got := EventTokenMinimum(tcase.ev1, tcase.ev2)
		if tcase.expected == nil && got != nil {
			t.Errorf("expected nil result for Minimum(%v, %v) but got: %v", tcase.ev1, tcase.ev2, got)
			continue
		}
		if !proto2.Equal(got, tcase.expected) {
			t.Errorf("got %v but expected %v for Minimum(%v, %v)", got, tcase.expected, tcase.ev1, tcase.ev2)
		}
	}
}
