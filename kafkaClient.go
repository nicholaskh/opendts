package replicator

type KafkaMessage struct {
	Table       string                   `json:"table"`
	Database    string                   `json:"database"`
	Gtid        string                   `json:"gtid,omitempty"`
	ExecuteTime int64                    `json:"es"`
	Ts          int64                    `json:"ts"`
	PkNames     []string                 `json:"pkNames"`
	Sql         string                   `json:"sql"`
	IsDDL       bool                     `json:"isDdl"`
	Type        string                   `json:"type"`
	Data        []map[string]interface{} `json:"data"`
	Old         []map[string]interface{} `json:"old,omitempty"`
}

type KafkaMessageMaxwell struct {
	Table    string                 `json:"table"`
	Database string                 `json:"database"`
	Gtid     string                 `json:"gtid,omitempty"`
	Ts       int64                  `json:"ts"`
	Type     string                 `json:"type"`
	Data     map[string]interface{} `json:"data"`
	Old      map[string]interface{} `json:"old,omitempty"`
}
