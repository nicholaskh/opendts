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

	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
)

// This file provides wrappers and support
// functions for proto.Type.

// These bit flags can be used to query on the
// common properties of types.
const (
	flagIsIntegral = int(proto.Flag_ISINTEGRAL)
	flagIsUnsigned = int(proto.Flag_ISUNSIGNED)
	flagIsFloat    = int(proto.Flag_ISFLOAT)
	flagIsQuoted   = int(proto.Flag_ISQUOTED)
	flagIsText     = int(proto.Flag_ISTEXT)
	flagIsBinary   = int(proto.Flag_ISBINARY)
)

// IsIntegral returns true if proto.Type is an integral
// (signed/unsigned) that can be represented using
// up to 64 binary bits.
// If you have a Value object, use its member function.
func IsIntegral(t proto.Type) bool {
	return int(t)&flagIsIntegral == flagIsIntegral
}

// IsSigned returns true if proto.Type is a signed integral.
// If you have a Value object, use its member function.
func IsSigned(t proto.Type) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral
}

// IsUnsigned returns true if proto.Type is an unsigned integral.
// Caution: this is not the same as !IsSigned.
// If you have a Value object, use its member function.
func IsUnsigned(t proto.Type) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral|flagIsUnsigned
}

// IsFloat returns true is proto.Type is a floating point.
// If you have a Value object, use its member function.
func IsFloat(t proto.Type) bool {
	return int(t)&flagIsFloat == flagIsFloat
}

// IsQuoted returns true if proto.Type is a quoted text or binary.
// If you have a Value object, use its member function.
func IsQuoted(t proto.Type) bool {
	return (int(t)&flagIsQuoted == flagIsQuoted) && t != Bit
}

// IsText returns true if proto.Type is a text.
// If you have a Value object, use its member function.
func IsText(t proto.Type) bool {
	return int(t)&flagIsText == flagIsText
}

// IsBinary returns true if proto.Type is a binary.
// If you have a Value object, use its member function.
func IsBinary(t proto.Type) bool {
	return int(t)&flagIsBinary == flagIsBinary
}

// IsNumber returns true if the type is any type of number.
func IsNumber(t proto.Type) bool {
	return IsIntegral(t) || IsFloat(t) || t == Decimal
}

// Vitess data types. These are idiomatically
// named synonyms for the proto.Type values.
// Although these constants are interchangeable,
// they should be treated as different from proto.Type.
// Use the synonyms only to refer to the type in Value.
// For proto variables, use the proto.Type constants
// instead.
// The following conditions are non-overlapping
// and cover all types: IsSigned(), IsUnsigned(),
// IsFloat(), IsQuoted(), Null, Decimal, Expression, Bit
// Also, IsIntegral() == (IsSigned()||IsUnsigned()).
// TestCategory needs to be updated accordingly if
// you add a new type.
// If IsBinary or IsText is true, then IsQuoted is
// also true. But there are IsQuoted types that are
// neither binary or text.
// proto.Type_TUPLE is not included in this list
// because it's not a valid Value type.
// TODO(sougou): provide a categorization function
// that returns enums, which will allow for cleaner
// switch statements for those who want to cover types
// by their category.
const (
	Null       = proto.Type_NULL_TYPE
	Int8       = proto.Type_INT8
	Uint8      = proto.Type_UINT8
	Int16      = proto.Type_INT16
	Uint16     = proto.Type_UINT16
	Int24      = proto.Type_INT24
	Uint24     = proto.Type_UINT24
	Int32      = proto.Type_INT32
	Uint32     = proto.Type_UINT32
	Int64      = proto.Type_INT64
	Uint64     = proto.Type_UINT64
	Float32    = proto.Type_FLOAT32
	Float64    = proto.Type_FLOAT64
	Timestamp  = proto.Type_TIMESTAMP
	Date       = proto.Type_DATE
	Time       = proto.Type_TIME
	Datetime   = proto.Type_DATETIME
	Year       = proto.Type_YEAR
	Decimal    = proto.Type_DECIMAL
	Text       = proto.Type_TEXT
	Blob       = proto.Type_BLOB
	VarChar    = proto.Type_VARCHAR
	VarBinary  = proto.Type_VARBINARY
	Char       = proto.Type_CHAR
	Binary     = proto.Type_BINARY
	Bit        = proto.Type_BIT
	Enum       = proto.Type_ENUM
	Set        = proto.Type_SET
	Geometry   = proto.Type_GEOMETRY
	TypeJSON   = proto.Type_JSON
	Expression = proto.Type_EXPRESSION
)

// bit-shift the mysql flags by two byte so we
// can merge them with the mysql or vitess types.
const (
	mysqlUnsigned = 32
	mysqlBinary   = 128
	mysqlEnum     = 256
	mysqlSet      = 2048
)

// If you add to this map, make sure you add a test case
// in tabletserver/endtoend.
var mysqlToType = map[int64]proto.Type{
	0:   Decimal,
	1:   Int8,
	2:   Int16,
	3:   Int32,
	4:   Float32,
	5:   Float64,
	6:   Null,
	7:   Timestamp,
	8:   Int64,
	9:   Int24,
	10:  Date,
	11:  Time,
	12:  Datetime,
	13:  Year,
	15:  VarChar,
	16:  Bit,
	17:  Timestamp,
	18:  Datetime,
	19:  Time,
	245: TypeJSON,
	246: Decimal,
	247: Enum,
	248: Set,
	249: Text,
	250: Text,
	251: Text,
	252: Text,
	253: VarChar,
	254: Char,
	255: Geometry,
}

// modifyType modifies the vitess type based on the
// mysql flag. The function checks specific flags based
// on the type. This allows us to ignore stray flags
// that MySQL occasionally sets.
func modifyType(typ proto.Type, flags int64) proto.Type {
	switch typ {
	case Int8:
		if flags&mysqlUnsigned != 0 {
			return Uint8
		}
	case Int16:
		if flags&mysqlUnsigned != 0 {
			return Uint16
		}
	case Int32:
		if flags&mysqlUnsigned != 0 {
			return Uint32
		}
	case Int64:
		if flags&mysqlUnsigned != 0 {
			return Uint64
		}
	case Int24:
		if flags&mysqlUnsigned != 0 {
			return Uint24
		}
	case Text:
		if flags&mysqlBinary != 0 {
			return Blob
		}
	case VarChar:
		if flags&mysqlBinary != 0 {
			return VarBinary
		}
	case Char:
		if flags&mysqlBinary != 0 {
			return Binary
		}
		if flags&mysqlEnum != 0 {
			return Enum
		}
		if flags&mysqlSet != 0 {
			return Set
		}
	case Year:
		if flags&mysqlBinary != 0 {
			return VarBinary
		}
	}
	return typ
}

// MySQLToType computes the vitess type from mysql type and flags.
func MySQLToType(mysqlType, flags int64) (typ proto.Type, err error) {
	result, ok := mysqlToType[mysqlType]
	if !ok {
		return 0, fmt.Errorf("unsupported type: %d", mysqlType)
	}
	return modifyType(result, flags), nil
}

// AreTypesEquivalent returns whether two types are equivalent.
func AreTypesEquivalent(mysqlTypeFromBinlog, mysqlTypeFromSchema proto.Type) bool {
	return (mysqlTypeFromBinlog == mysqlTypeFromSchema) ||
		(mysqlTypeFromBinlog == VarChar && mysqlTypeFromSchema == VarBinary) ||
		// Binlog only has base type. But doesn't have per-column-flags to differentiate
		// various logical types. For Binary, Enum, Set types, binlog only returns Char
		// as data type.
		(mysqlTypeFromBinlog == Char && mysqlTypeFromSchema == Binary) ||
		(mysqlTypeFromBinlog == Char && mysqlTypeFromSchema == Enum) ||
		(mysqlTypeFromBinlog == Char && mysqlTypeFromSchema == Set) ||
		(mysqlTypeFromBinlog == Text && mysqlTypeFromSchema == Blob) ||
		(mysqlTypeFromBinlog == Int8 && mysqlTypeFromSchema == Uint8) ||
		(mysqlTypeFromBinlog == Int16 && mysqlTypeFromSchema == Uint16) ||
		(mysqlTypeFromBinlog == Int24 && mysqlTypeFromSchema == Uint24) ||
		(mysqlTypeFromBinlog == Int32 && mysqlTypeFromSchema == Uint32) ||
		(mysqlTypeFromBinlog == Int64 && mysqlTypeFromSchema == Uint64)
}

// typeToMySQL is the reverse of mysqlToType.
var typeToMySQL = map[proto.Type]struct {
	typ   int64
	flags int64
}{
	Int8:      {typ: 1},
	Uint8:     {typ: 1, flags: mysqlUnsigned},
	Int16:     {typ: 2},
	Uint16:    {typ: 2, flags: mysqlUnsigned},
	Int32:     {typ: 3},
	Uint32:    {typ: 3, flags: mysqlUnsigned},
	Float32:   {typ: 4},
	Float64:   {typ: 5},
	Null:      {typ: 6, flags: mysqlBinary},
	Timestamp: {typ: 7},
	Int64:     {typ: 8},
	Uint64:    {typ: 8, flags: mysqlUnsigned},
	Int24:     {typ: 9},
	Uint24:    {typ: 9, flags: mysqlUnsigned},
	Date:      {typ: 10, flags: mysqlBinary},
	Time:      {typ: 11, flags: mysqlBinary},
	Datetime:  {typ: 12, flags: mysqlBinary},
	Year:      {typ: 13, flags: mysqlUnsigned},
	Bit:       {typ: 16, flags: mysqlUnsigned},
	TypeJSON:  {typ: 245},
	Decimal:   {typ: 246},
	Text:      {typ: 252},
	Blob:      {typ: 252, flags: mysqlBinary},
	VarChar:   {typ: 253},
	VarBinary: {typ: 253, flags: mysqlBinary},
	Char:      {typ: 254},
	Binary:    {typ: 254, flags: mysqlBinary},
	Enum:      {typ: 254, flags: mysqlEnum},
	Set:       {typ: 254, flags: mysqlSet},
	Geometry:  {typ: 255},
}

// TypeToMySQL returns the equivalent mysql type and flag for a vitess type.
func TypeToMySQL(typ proto.Type) (mysqlType, flags int64) {
	val := typeToMySQL[typ]
	return val.typ, val.flags
}
