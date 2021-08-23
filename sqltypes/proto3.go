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
	proto2 "github.com/golang/protobuf/proto"

	"github.com/nicholaskh/opendts/proto"
)

// This file contains the proto3 conversion functions for the structures
// defined here.

// RowToProto3 converts []Value to proto3.
func RowToProto3(row []Value) *proto.Row {
	result := &proto.Row{}
	result.Lengths = make([]int64, 0, len(row))
	total := 0
	for _, c := range row {
		if c.IsNull() {
			result.Lengths = append(result.Lengths, -1)
			continue
		}
		length := c.Len()
		result.Lengths = append(result.Lengths, int64(length))
		total += length
	}
	result.Values = make([]byte, 0, total)
	for _, c := range row {
		if c.IsNull() {
			continue
		}
		result.Values = append(result.Values, c.Raw()...)
	}
	return result
}

// RowsToProto3 converts [][]Value to proto3.
func RowsToProto3(rows [][]Value) []*proto.Row {
	if len(rows) == 0 {
		return nil
	}

	result := make([]*proto.Row, len(rows))
	for i, r := range rows {
		result[i] = RowToProto3(r)
	}
	return result
}

// proto3ToRows converts a proto3 rows to [][]Value. The function is private
// because it uses the trusted API.
func proto3ToRows(fields []*proto.Field, rows []*proto.Row) [][]Value {
	if len(rows) == 0 {
		// TODO(sougou): This is needed for backward compatibility.
		// Remove when it's not needed any more.
		return [][]Value{}
	}

	result := make([][]Value, len(rows))
	for i, r := range rows {
		result[i] = MakeRowTrusted(fields, r)
	}
	return result
}

// ResultToProto3 converts Result to proto3.
func ResultToProto3(qr *Result) *proto.QueryResult {
	if qr == nil {
		return nil
	}
	return &proto.QueryResult{
		Fields:       qr.Fields,
		RowsAffected: qr.RowsAffected,
		InsertId:     qr.InsertID,
		Rows:         RowsToProto3(qr.Rows),
	}
}

// Proto3ToResult converts a proto3 Result to an internal data structure. This function
// should be used only if the field info is populated in qr.
func Proto3ToResult(qr *proto.QueryResult) *Result {
	if qr == nil {
		return nil
	}
	return &Result{
		Fields:       qr.Fields,
		RowsAffected: qr.RowsAffected,
		InsertID:     qr.InsertId,
		Rows:         proto3ToRows(qr.Fields, qr.Rows),
	}
}

// CustomProto3ToResult converts a proto3 Result to an internal data structure. This function
// takes a separate fields input because not all QueryResults contain the field info.
// In particular, only the first packet of streaming queries contain the field info.
func CustomProto3ToResult(fields []*proto.Field, qr *proto.QueryResult) *Result {
	if qr == nil {
		return nil
	}
	return &Result{
		Fields:       qr.Fields,
		RowsAffected: qr.RowsAffected,
		InsertID:     qr.InsertId,
		Rows:         proto3ToRows(fields, qr.Rows),
	}
}

// ResultsToProto3 converts []Result to proto3.
func ResultsToProto3(qr []Result) []*proto.QueryResult {
	if len(qr) == 0 {
		return nil
	}
	result := make([]*proto.QueryResult, len(qr))
	for i, q := range qr {
		result[i] = ResultToProto3(&q)
	}
	return result
}

// Proto3ToResults converts proto3 results to []Result.
func Proto3ToResults(qr []*proto.QueryResult) []Result {
	if len(qr) == 0 {
		return nil
	}
	result := make([]Result, len(qr))
	for i, q := range qr {
		result[i] = *Proto3ToResult(q)
	}
	return result
}

// QueryResponsesToProto3 converts []QueryResponse to proto3.
func QueryResponsesToProto3(qr []QueryResponse) []*proto.ResultWithError {
	if len(qr) == 0 {
		return nil
	}
	result := make([]*proto.ResultWithError, len(qr))
	for i, q := range qr {
		result[i] = &proto.ResultWithError{
			Result: ResultToProto3(q.QueryResult),
			Error:  q.QueryError,
		}
	}
	return result
}

// Proto3ToQueryReponses converts proto3 queryResponse to []QueryResponse.
func Proto3ToQueryReponses(qr []*proto.ResultWithError) []QueryResponse {
	if len(qr) == 0 {
		return nil
	}
	result := make([]QueryResponse, len(qr))
	for i, q := range qr {
		result[i] = QueryResponse{
			QueryResult: Proto3ToResult(q.Result),
			QueryError:  q.Error,
		}
	}
	return result
}

func Proto3ResultsEqual(r1, r2 []*proto.QueryResult) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i, r := range r1 {
		if !proto2.Equal(r, r2[i]) {
			return false
		}
	}
	return true
}

func Proto3QueryResponsesEqual(r1, r2 []*proto.ResultWithError) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i, r := range r1 {
		if !proto2.Equal(r, r2[i]) {
			return false
		}
	}
	return true
}

func Proto3ValuesEqual(v1, v2 []*proto.Value) bool {
	if len(v1) != len(v2) {
		return false
	}
	for i, v := range v1 {
		if !proto2.Equal(v, v2[i]) {
			return false
		}
	}
	return true
}
