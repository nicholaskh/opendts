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
	"fmt"
	"reflect"
	"strings"
	"testing"

	proto2 "github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/test_utils"
)

func TestProtoConversions(t *testing.T) {
	v := TestValue(Int64, "1")
	got := ValueToProto(v)
	want := &proto.Value{Type: Int64, Value: []byte("1")}
	if !proto2.Equal(got, want) {
		t.Errorf("ValueToProto: %v, want %v", got, want)
	}
	gotback := ProtoToValue(got)
	if !reflect.DeepEqual(gotback, v) {
		t.Errorf("ProtoToValue: %v, want %v", gotback, v)
	}
}

func TestBuildBindVariables(t *testing.T) {
	tcases := []struct {
		in  map[string]interface{}
		out map[string]*proto.BindVariable
		err string
	}{{
		in:  nil,
		out: nil,
	}, {
		in: map[string]interface{}{
			"k": int64(1),
		},
		out: map[string]*proto.BindVariable{
			"k": Int64BindVariable(1),
		},
	}, {
		in: map[string]interface{}{
			"k": byte(1),
		},
		err: "k: type uint8 not supported as bind var: 1",
	}}
	for _, tcase := range tcases {
		bindVars, err := BuildBindVariables(tcase.in)
		if err != nil {
			if err.Error() != tcase.err {
				t.Errorf("MapToBindVars(%v) error: %v, want %s", tcase.in, err, tcase.err)
			}
			continue
		}
		if tcase.err != "" {
			t.Errorf("MapToBindVars(%v) error: nil, want %s", tcase.in, tcase.err)
			continue
		}
		if !BindVariablesEqual(bindVars, tcase.out) {
			t.Errorf("MapToBindVars(%v): %v, want %v", tcase.in, bindVars, tcase.out)
		}
	}
}

func TestBuildBindVariable(t *testing.T) {
	tcases := []struct {
		in  interface{}
		out *proto.BindVariable
		err string
	}{{
		in: "aa",
		out: &proto.BindVariable{
			Type:  proto.Type_VARBINARY,
			Value: []byte("aa"),
		},
	}, {
		in: []byte("aa"),
		out: &proto.BindVariable{
			Type:  proto.Type_VARBINARY,
			Value: []byte("aa"),
		},
	}, {
		in: true,
		out: &proto.BindVariable{
			Type:  proto.Type_INT8,
			Value: []byte("1"),
		},
	}, {
		in: false,
		out: &proto.BindVariable{
			Type:  proto.Type_INT8,
			Value: []byte("0"),
		},
	}, {
		in: int(1),
		out: &proto.BindVariable{
			Type:  proto.Type_INT64,
			Value: []byte("1"),
		},
	}, {
		in: int64(1),
		out: &proto.BindVariable{
			Type:  proto.Type_INT64,
			Value: []byte("1"),
		},
	}, {
		in: uint64(1),
		out: &proto.BindVariable{
			Type:  proto.Type_UINT64,
			Value: []byte("1"),
		},
	}, {
		in: float64(1),
		out: &proto.BindVariable{
			Type:  proto.Type_FLOAT64,
			Value: []byte("1"),
		},
	}, {
		in:  nil,
		out: NullBindVariable,
	}, {
		in: MakeTrusted(Int64, []byte("1")),
		out: &proto.BindVariable{
			Type:  proto.Type_INT64,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_INT64,
			Value: []byte("1"),
		},
		out: &proto.BindVariable{
			Type:  proto.Type_INT64,
			Value: []byte("1"),
		},
	}, {
		in: []interface{}{"aa", int64(1)},
		out: &proto.BindVariable{
			Type: proto.Type_TUPLE,
			Values: []*proto.Value{{
				Type:  proto.Type_VARBINARY,
				Value: []byte("aa"),
			}, {
				Type:  proto.Type_INT64,
				Value: []byte("1"),
			}},
		},
	}, {
		in: []string{"aa", "bb"},
		out: &proto.BindVariable{
			Type: proto.Type_TUPLE,
			Values: []*proto.Value{{
				Type:  proto.Type_VARBINARY,
				Value: []byte("aa"),
			}, {
				Type:  proto.Type_VARBINARY,
				Value: []byte("bb"),
			}},
		},
	}, {
		in: [][]byte{[]byte("aa"), []byte("bb")},
		out: &proto.BindVariable{
			Type: proto.Type_TUPLE,
			Values: []*proto.Value{{
				Type:  proto.Type_VARBINARY,
				Value: []byte("aa"),
			}, {
				Type:  proto.Type_VARBINARY,
				Value: []byte("bb"),
			}},
		},
	}, {
		in: []int{1, 2},
		out: &proto.BindVariable{
			Type: proto.Type_TUPLE,
			Values: []*proto.Value{{
				Type:  proto.Type_INT64,
				Value: []byte("1"),
			}, {
				Type:  proto.Type_INT64,
				Value: []byte("2"),
			}},
		},
	}, {
		in: []int64{1, 2},
		out: &proto.BindVariable{
			Type: proto.Type_TUPLE,
			Values: []*proto.Value{{
				Type:  proto.Type_INT64,
				Value: []byte("1"),
			}, {
				Type:  proto.Type_INT64,
				Value: []byte("2"),
			}},
		},
	}, {
		in: []uint64{1, 2},
		out: &proto.BindVariable{
			Type: proto.Type_TUPLE,
			Values: []*proto.Value{{
				Type:  proto.Type_UINT64,
				Value: []byte("1"),
			}, {
				Type:  proto.Type_UINT64,
				Value: []byte("2"),
			}},
		},
	}, {
		in: []float64{1, 2},
		out: &proto.BindVariable{
			Type: proto.Type_TUPLE,
			Values: []*proto.Value{{
				Type:  proto.Type_FLOAT64,
				Value: []byte("1"),
			}, {
				Type:  proto.Type_FLOAT64,
				Value: []byte("2"),
			}},
		},
	}, {
		in:  byte(1),
		err: "type uint8 not supported as bind var: 1",
	}, {
		in:  []interface{}{1, byte(1)},
		err: "type uint8 not supported as bind var: 1",
	}}
	for _, tcase := range tcases {
		t.Run(fmt.Sprintf("%v", tcase.in), func(t *testing.T) {
			bv, err := BuildBindVariable(tcase.in)
			if tcase.err != "" {
				require.EqualError(t, err, tcase.err)
			} else {
				test_utils.MustMatch(t, tcase.out, bv, "binvar output did not match")
			}
		})
	}
}

func TestValidateBindVarables(t *testing.T) {
	tcases := []struct {
		in  map[string]*proto.BindVariable
		err string
	}{{
		in: map[string]*proto.BindVariable{
			"v": {
				Type:  proto.Type_INT64,
				Value: []byte("1"),
			},
		},
		err: "",
	}, {
		in: map[string]*proto.BindVariable{
			"v": {
				Type:  proto.Type_INT64,
				Value: []byte("a"),
			},
		},
		err: `v: strconv.ParseInt: parsing "a": invalid syntax`,
	}, {
		in: map[string]*proto.BindVariable{
			"v": {
				Type: proto.Type_TUPLE,
				Values: []*proto.Value{{
					Type:  Int64,
					Value: []byte("a"),
				}},
			},
		},
		err: `v: strconv.ParseInt: parsing "a": invalid syntax`,
	}}
	for _, tcase := range tcases {
		err := ValidateBindVariables(tcase.in)
		if tcase.err != "" {
			if err == nil || err.Error() != tcase.err {
				t.Errorf("ValidateBindVars(%v): %v, want %s", tcase.in, err, tcase.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("ValidateBindVars(%v): %v, want nil", tcase.in, err)
		}
	}
}

func TestValidateBindVariable(t *testing.T) {
	testcases := []struct {
		in  *proto.BindVariable
		err string
	}{{
		in: &proto.BindVariable{
			Type:  proto.Type_INT8,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_INT16,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_INT24,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_INT32,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_INT64,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_UINT8,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_UINT16,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_UINT24,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_UINT32,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_UINT64,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_FLOAT32,
			Value: []byte("1.00"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_FLOAT64,
			Value: []byte("1.00"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_DECIMAL,
			Value: []byte("1.00"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_TIMESTAMP,
			Value: []byte("2012-02-24 23:19:43"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_DATE,
			Value: []byte("2012-02-24"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_TIME,
			Value: []byte("23:19:43"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_DATETIME,
			Value: []byte("2012-02-24 23:19:43"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_YEAR,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_TEXT,
			Value: []byte("a"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_BLOB,
			Value: []byte("a"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_VARCHAR,
			Value: []byte("a"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_BINARY,
			Value: []byte("a"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_CHAR,
			Value: []byte("a"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_BIT,
			Value: []byte("1"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_ENUM,
			Value: []byte("a"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_SET,
			Value: []byte("a"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_VARBINARY,
			Value: []byte("a"),
		},
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_INT64,
			Value: []byte(InvalidNeg),
		},
		err: "out of range",
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_INT64,
			Value: []byte(InvalidPos),
		},
		err: "out of range",
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_UINT64,
			Value: []byte("-1"),
		},
		err: "invalid syntax",
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_UINT64,
			Value: []byte(InvalidPos),
		},
		err: "out of range",
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_FLOAT64,
			Value: []byte("a"),
		},
		err: "invalid syntax",
	}, {
		in: &proto.BindVariable{
			Type:  proto.Type_EXPRESSION,
			Value: []byte("a"),
		},
		err: "invalid type specified for MakeValue: EXPRESSION",
	}, {
		in: &proto.BindVariable{
			Type: proto.Type_TUPLE,
			Values: []*proto.Value{{
				Type:  proto.Type_INT64,
				Value: []byte("1"),
			}},
		},
	}, {
		in: &proto.BindVariable{
			Type: proto.Type_TUPLE,
		},
		err: "empty tuple is not allowed",
	}, {
		in: &proto.BindVariable{
			Type: proto.Type_TUPLE,
			Values: []*proto.Value{{
				Type: proto.Type_TUPLE,
			}},
		},
		err: "tuple not allowed inside another tuple",
	}}
	for _, tcase := range testcases {
		err := ValidateBindVariable(tcase.in)
		if tcase.err != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("ValidateBindVar(%v) error: %v, must contain %v", tcase.in, err, tcase.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("ValidateBindVar(%v) error: %v", tcase.in, err)
		}
	}

	// Special case: nil bind var.
	err := ValidateBindVariable(nil)
	want := "bind variable is nil"
	if err == nil || err.Error() != want {
		t.Errorf("ValidateBindVar(nil) error: %v, want %s", err, want)
	}
}

func TestBindVariableToValue(t *testing.T) {
	v, err := BindVariableToValue(Int64BindVariable(1))
	require.NoError(t, err)
	want := MakeTrusted(proto.Type_INT64, []byte("1"))
	if !reflect.DeepEqual(v, want) {
		t.Errorf("BindVarToValue(1): %v, want %v", v, want)
	}

	v, err = BindVariableToValue(&proto.BindVariable{Type: proto.Type_TUPLE})
	wantErr := "cannot convert a TUPLE bind var into a value"
	if err == nil || err.Error() != wantErr {
		t.Errorf(" BindVarToValue(TUPLE): (%v, %v), want %s", v, err, wantErr)
	}
}

func TestBindVariablesEqual(t *testing.T) {
	bv1 := map[string]*proto.BindVariable{
		"k": {
			Type:  proto.Type_INT64,
			Value: []byte("1"),
		},
	}
	bv2 := map[string]*proto.BindVariable{
		"k": {
			Type:  proto.Type_INT64,
			Value: []byte("1"),
		},
	}
	bv3 := map[string]*proto.BindVariable{
		"k": {
			Type:  proto.Type_INT64,
			Value: []byte("1"),
		},
	}
	if !BindVariablesEqual(bv1, bv2) {
		t.Errorf("%v != %v, want equal", bv1, bv2)
	}
	if !BindVariablesEqual(bv1, bv3) {
		t.Errorf("%v = %v, want not equal", bv1, bv3)
	}
}

func TestBindVariablesFormat(t *testing.T) {
	tupleBindVar, err := BuildBindVariable([]int64{1, 2})
	if err != nil {
		t.Fatalf("failed to create a tuple bind var: %v", err)
	}

	bindVariables := map[string]*proto.BindVariable{
		"key_1": StringBindVariable("val_1"),
		"key_2": Int64BindVariable(789),
		"key_3": BytesBindVariable([]byte("val_3")),
		"key_4": tupleBindVar,
	}

	formattedStr := FormatBindVariables(bindVariables, true /* full */, false /* asJSON */)
	if !strings.Contains(formattedStr, "key_1") ||
		!strings.Contains(formattedStr, "val_1") {
		t.Fatalf("bind variable 'key_1': 'val_1' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_2") ||
		!strings.Contains(formattedStr, "789") {
		t.Fatalf("bind variable 'key_2': '789' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_3") || !strings.Contains(formattedStr, "val_3") {
		t.Fatalf("bind variable 'key_3': 'val_3' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_4") ||
		!strings.Contains(formattedStr, "values:<type:INT64 value:\"1\" > values:<type:INT64 value:\"2\" >") {
		t.Fatalf("bind variable 'key_4': (1, 2) is not formatted")
	}

	formattedStr = FormatBindVariables(bindVariables, false /* full */, false /* asJSON */)
	if !strings.Contains(formattedStr, "key_1") {
		t.Fatalf("bind variable 'key_1' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_2") ||
		!strings.Contains(formattedStr, "789") {
		t.Fatalf("bind variable 'key_2': '789' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_3") || !strings.Contains(formattedStr, "5 bytes") {
		t.Fatalf("bind variable 'key_3' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_4") || !strings.Contains(formattedStr, "2 items") {
		t.Fatalf("bind variable 'key_4' is not formatted")
	}

	formattedStr = FormatBindVariables(bindVariables, true /* full */, true /* asJSON */)
	t.Logf("%q", formattedStr)
	if !strings.Contains(formattedStr, "\"key_1\": {\"type\": \"VARBINARY\", \"value\": \"val_1\"}") {
		t.Fatalf("bind variable 'key_1' is not formatted")
	}

	if !strings.Contains(formattedStr, "\"key_2\": {\"type\": \"INT64\", \"value\": 789}") {
		t.Fatalf("bind variable 'key_2' is not formatted")
	}

	if !strings.Contains(formattedStr, "\"key_3\": {\"type\": \"VARBINARY\", \"value\": \"val_3\"}") {
		t.Fatalf("bind variable 'key_3' is not formatted")
	}

	if !strings.Contains(formattedStr, "\"key_4\": {\"type\": \"TUPLE\", \"value\": \"\"}") {
		t.Fatalf("bind variable 'key_4' is not formatted")
	}

	formattedStr = FormatBindVariables(bindVariables, false /* full */, true /* asJSON */)
	if !strings.Contains(formattedStr, "\"key_1\": {\"type\": \"VARBINARY\", \"value\": \"5 bytes\"}") {
		t.Fatalf("bind variable 'key_1' is not formatted")
	}

	if !strings.Contains(formattedStr, "\"key_2\": {\"type\": \"INT64\", \"value\": 789}") {
		t.Fatalf("bind variable 'key_2' is not formatted")
	}

	if !strings.Contains(formattedStr, "\"key_3\": {\"type\": \"VARBINARY\", \"value\": \"5 bytes\"}") {
		t.Fatalf("bind variable 'key_3' is not formatted")
	}

	if !strings.Contains(formattedStr, "\"key_4\": {\"type\": \"VARBINARY\", \"value\": \"2 items\"}") {
		t.Fatalf("bind variable 'key_4' is not formatted")
	}
}
