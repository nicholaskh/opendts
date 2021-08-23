package vo

type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    Data   `json:"data"`
}

type Data struct {
	Id int `json:"id,omitempty"`
}

type TaskReq struct {
	Source    *Mysql     `json:"source"`
	Dest      *Mysql     `json:"dest"`
	Kafka     *Kafka     `json:"kafka"`
	Filters   []*Filter  `json:"filters"`
	RateLimit *RateLimit `json:"rate_limit"`
	IsIncr    bool       `json:"incr"`
}

type TaskEntity struct {
	Id              int    `json:"id"`
	SourceAddr      string `json:"source_addr"`
	DestAddr        string `json:"dest_addr"`
	Pos             string `json:"pos"`
	UpdatedAt       string `json:"updated_at"`
	TransactionTime string `json:"transaction_time"`
	State           string `json:"state"`
	Message         string `json:"message"`
	ServiceAddr     string `json:"service_addr"`
}

type Filter struct {
	Dbname       string `json:"dbname"`
	Match        string `json:"match"`
	Filter       string `json:"filter"`
	TargetDbname string `json:"target_dbname"`
}

type RateLimit struct {
	Rate     int `json:"rate"`
	Capacity int `json:"capacity"`
}

type Mysql struct {
	Host             string `json:"host"`
	Port             int    `json:"port"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	DB               string `json:"db"`
	Charset          string `json:"charset"`
	ConnectTimeoutMs uint64 `json:"connect_timeout_ms"`
}

type Kafka struct {
	Addr                        string   `json:"addr"`
	Topic                       string   `json:"topic"`
	FlatFormat                  bool     `json:"flat_format"`
	MaxwellFormat               bool     `json:"maxwell_format"`
	TopicMode                   uint8    `json:"topic_mode"`
	PartitionMode               string   `json:"partition_mode"`
	PartitionColumns            []string `json:"partition_columns"`
	ExcludeColumns              []string `json:"exclude_columns"`
	WithGtid                    bool     `json:"with_gtid"`
	ProducerPartitionByFallback string   `json:"producer_partition_by_fallback"`
}

type SubmitTaskReq struct {
	Task TaskReq `json:"task"`
}

type SubmitTaskRes struct {
	Addr string `json:"addr"`
	Id   int    `json:"id"`
}

type RunTaskReq struct {
	Task TaskReq `json:"task"`
}

type RunTaskRes struct {
	Id int `json:"id"`
}

type ListTasksRes struct {
	Tasks []*TaskEntity `json:"tasks"`
}

type DetailTaskRes struct {
	Task *TaskEntity `json:"task"`
}

type RecoverTaskRes struct {
}

type AlterTaskReq struct {
	Task TaskReq `json:"task"`
}

type AlterTaskRes struct {
}

type DeleteTaskRes struct {
}

type PauseTaskRes struct {
}

type DispatchTaskRes struct {
}
