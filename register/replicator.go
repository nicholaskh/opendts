package register

type ReplicatorInfo struct {
	Addr      string `json:"addr"`
	StartTime string `json:"start_time"`
	Pid       int    `json:"pid"`
	Pwd       string `json:"pwd"`
}

func (r *ReplicatorInfo) Encode() []byte {
	return JSONEncode(r)
}
