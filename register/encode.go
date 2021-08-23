package register

import (
	"encoding/json"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/log"
)

// JSONEncode return json encoding of v
func JSONEncode(v interface{}) []byte {
	b, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		log.Fatal("encode to json failed, %v", err)
		return nil
	}
	return b
}

// JSONDecode parses the JSON-encoded data and stores the result in the value pointed to by v
func JSONDecode(v interface{}, data []byte) error {
	return json.Unmarshal(data, v)
}
