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
	"bytes"
	"errors"
	"fmt"
	"strconv"

	proto2 "github.com/golang/protobuf/proto"

	"github.com/nicholaskh/opendts/proto"
)

var (
	// BvSchemaName is bind variable to be sent down to vttablet for schema name.
	BvSchemaName = "__vtschemaname"

	// BvReplaceSchemaName is bind variable to be sent down to vttablet to replace schema name.
	BvReplaceSchemaName = "__replacevtschemaname"

	// NullBindVariable is a bindvar with NULL value.
	NullBindVariable = &proto.BindVariable{Type: proto.Type_NULL_TYPE}
)

// ValueToProto converts Value to a *proto.Value.
func ValueToProto(v Value) *proto.Value {
	return &proto.Value{Type: v.typ, Value: v.val}
}

// ProtoToValue converts a *proto.Value to a Value.
func ProtoToValue(v *proto.Value) Value {
	return MakeTrusted(v.Type, v.Value)
}

// BuildBindVariables builds a map[string]*proto.BindVariable from a map[string]interface{}.
func BuildBindVariables(in map[string]interface{}) (map[string]*proto.BindVariable, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make(map[string]*proto.BindVariable, len(in))
	for k, v := range in {
		bv, err := BuildBindVariable(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", k, err)
		}
		out[k] = bv
	}
	return out, nil
}

// Int8BindVariable converts an int8 to a bind var.
func Int8BindVariable(v int8) *proto.BindVariable {
	return ValueBindVariable(NewInt8(v))
}

// Int32BindVariable converts an int32 to a bind var.
func Int32BindVariable(v int32) *proto.BindVariable {
	return ValueBindVariable(NewInt32(v))
}

// BoolBindVariable converts an bool to a int32 bind var.
func BoolBindVariable(v bool) *proto.BindVariable {
	if v {
		return Int32BindVariable(1)
	}
	return Int32BindVariable(0)
}

// Int64BindVariable converts an int64 to a bind var.
func Int64BindVariable(v int64) *proto.BindVariable {
	return ValueBindVariable(NewInt64(v))
}

// Uint64BindVariable converts a uint64 to a bind var.
func Uint64BindVariable(v uint64) *proto.BindVariable {
	return ValueBindVariable(NewUint64(v))
}

// Float64BindVariable converts a float64 to a bind var.
func Float64BindVariable(v float64) *proto.BindVariable {
	return ValueBindVariable(NewFloat64(v))
}

// StringBindVariable converts a string to a bind var.
func StringBindVariable(v string) *proto.BindVariable {
	return ValueBindVariable(NewVarBinary(v))
}

// BytesBindVariable converts a []byte to a bind var.
func BytesBindVariable(v []byte) *proto.BindVariable {
	return &proto.BindVariable{Type: VarBinary, Value: v}
}

// ValueBindVariable converts a Value to a bind var.
func ValueBindVariable(v Value) *proto.BindVariable {
	return &proto.BindVariable{Type: v.typ, Value: v.val}
}

// BuildBindVariable builds a *proto.BindVariable from a valid input type.
func BuildBindVariable(v interface{}) (*proto.BindVariable, error) {
	switch v := v.(type) {
	case string:
		return BytesBindVariable([]byte(v)), nil
	case []byte:
		return BytesBindVariable(v), nil
	case bool:
		if v {
			return Int8BindVariable(1), nil
		}
		return Int8BindVariable(0), nil
	case int:
		return &proto.BindVariable{
			Type:  proto.Type_INT64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int64:
		return Int64BindVariable(v), nil
	case uint64:
		return Uint64BindVariable(v), nil
	case float64:
		return Float64BindVariable(v), nil
	case nil:
		return NullBindVariable, nil
	case Value:
		return ValueBindVariable(v), nil
	case *proto.BindVariable:
		return v, nil
	case []interface{}:
		bv := &proto.BindVariable{
			Type:   proto.Type_TUPLE,
			Values: make([]*proto.Value, len(v)),
		}
		values := make([]proto.Value, len(v))
		for i, lv := range v {
			lbv, err := BuildBindVariable(lv)
			if err != nil {
				return nil, err
			}
			values[i].Type = lbv.Type
			values[i].Value = lbv.Value
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []string:
		bv := &proto.BindVariable{
			Type:   proto.Type_TUPLE,
			Values: make([]*proto.Value, len(v)),
		}
		values := make([]proto.Value, len(v))
		for i, lv := range v {
			values[i].Type = proto.Type_VARBINARY
			values[i].Value = []byte(lv)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case [][]byte:
		bv := &proto.BindVariable{
			Type:   proto.Type_TUPLE,
			Values: make([]*proto.Value, len(v)),
		}
		values := make([]proto.Value, len(v))
		for i, lv := range v {
			values[i].Type = proto.Type_VARBINARY
			values[i].Value = lv
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []int:
		bv := &proto.BindVariable{
			Type:   proto.Type_TUPLE,
			Values: make([]*proto.Value, len(v)),
		}
		values := make([]proto.Value, len(v))
		for i, lv := range v {
			values[i].Type = proto.Type_INT64
			values[i].Value = strconv.AppendInt(nil, int64(lv), 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []int64:
		bv := &proto.BindVariable{
			Type:   proto.Type_TUPLE,
			Values: make([]*proto.Value, len(v)),
		}
		values := make([]proto.Value, len(v))
		for i, lv := range v {
			values[i].Type = proto.Type_INT64
			values[i].Value = strconv.AppendInt(nil, lv, 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []uint64:
		bv := &proto.BindVariable{
			Type:   proto.Type_TUPLE,
			Values: make([]*proto.Value, len(v)),
		}
		values := make([]proto.Value, len(v))
		for i, lv := range v {
			values[i].Type = proto.Type_UINT64
			values[i].Value = strconv.AppendUint(nil, lv, 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []float64:
		bv := &proto.BindVariable{
			Type:   proto.Type_TUPLE,
			Values: make([]*proto.Value, len(v)),
		}
		values := make([]proto.Value, len(v))
		for i, lv := range v {
			values[i].Type = proto.Type_FLOAT64
			values[i].Value = strconv.AppendFloat(nil, lv, 'g', -1, 64)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []Value:
		bv := &proto.BindVariable{
			Type:   proto.Type_TUPLE,
			Values: make([]*proto.Value, len(v)),
		}
		values := make([]proto.Value, len(v))
		for i, lv := range v {
			lbv, err := BuildBindVariable(lv)
			if err != nil {
				return nil, err
			}
			values[i].Type = lbv.Type
			values[i].Value = lbv.Value
			bv.Values[i] = &values[i]
		}
		return bv, nil
	}
	return nil, fmt.Errorf("type %T not supported as bind var: %v", v, v)
}

// ValidateBindVariables validates a map[string]*proto.BindVariable.
func ValidateBindVariables(bv map[string]*proto.BindVariable) error {
	for k, v := range bv {
		if err := ValidateBindVariable(v); err != nil {
			return fmt.Errorf("%s: %v", k, err)
		}
	}
	return nil
}

// ValidateBindVariable returns an error if the bind variable has inconsistent
// fields.
func ValidateBindVariable(bv *proto.BindVariable) error {
	if bv == nil {
		return errors.New("bind variable is nil")
	}

	if bv.Type == proto.Type_TUPLE {
		if len(bv.Values) == 0 {
			return errors.New("empty tuple is not allowed")
		}
		for _, val := range bv.Values {
			if val.Type == proto.Type_TUPLE {
				return errors.New("tuple not allowed inside another tuple")
			}
			if err := ValidateBindVariable(&proto.BindVariable{Type: val.Type, Value: val.Value}); err != nil {
				return err
			}
		}
		return nil
	}

	// If NewValue succeeds, the value is valid.
	_, err := NewValue(bv.Type, bv.Value)
	return err
}

// BindVariableToValue converts a bind var into a Value.
func BindVariableToValue(bv *proto.BindVariable) (Value, error) {
	if bv.Type == proto.Type_TUPLE {
		return NULL, errors.New("cannot convert a TUPLE bind var into a value")
	}
	return MakeTrusted(bv.Type, bv.Value), nil
}

func BindVariablesEqual(x, y map[string]*proto.BindVariable) bool {
	return proto2.Equal(&proto.BoundQuery{BindVariables: x}, &proto.BoundQuery{BindVariables: y})
}

// CopyBindVariables returns a shallow-copy of the given bindVariables map.
func CopyBindVariables(bindVariables map[string]*proto.BindVariable) map[string]*proto.BindVariable {
	result := make(map[string]*proto.BindVariable, len(bindVariables))
	for key, value := range bindVariables {
		result[key] = value
	}
	return result
}

// FormatBindVariables returns a string representation of the
// bind variables.
//
// If full is false, then large string or tuple values are truncated
// to only print the lengths.
//
// If asJson is true, then the resulting string is a valid JSON
// representation, otherwise it is the golang printed map representation.
func FormatBindVariables(bindVariables map[string]*proto.BindVariable, full, asJSON bool) string {
	var out map[string]*proto.BindVariable
	if full {
		out = bindVariables
	} else {
		// NOTE(szopa): I am getting rid of potentially large bind
		// variables.
		out = make(map[string]*proto.BindVariable)
		for k, v := range bindVariables {
			if IsIntegral(v.Type) || IsFloat(v.Type) {
				out[k] = v
			} else if v.Type == proto.Type_TUPLE {
				out[k] = StringBindVariable(fmt.Sprintf("%v items", len(v.Values)))
			} else {
				out[k] = StringBindVariable(fmt.Sprintf("%v bytes", len(v.Value)))
			}
		}
	}

	if asJSON {
		var buf bytes.Buffer
		buf.WriteString("{")
		first := true
		for k, v := range out {
			if !first {
				buf.WriteString(", ")
			} else {
				first = false
			}
			if IsIntegral(v.Type) || IsFloat(v.Type) {
				fmt.Fprintf(&buf, "%q: {\"type\": %q, \"value\": %v}", k, v.Type, string(v.Value))
			} else {
				fmt.Fprintf(&buf, "%q: {\"type\": %q, \"value\": %q}", k, v.Type, string(v.Value))
			}
		}
		buf.WriteString("}")
		return buf.String()
	}

	return fmt.Sprintf("%v", out)
}
