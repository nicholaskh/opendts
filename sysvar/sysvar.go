package sysvar

type SystemVariable struct {
	// IsBoolean is used to signal necessary type coercion so that strings
	// and numbers can be evaluated to a boolean value
	IsBoolean bool

	// IdentifierAsString allows identifiers (a.k.a. ColName) from the AST to be handled as if they are strings.
	// SET transaction_mode = two_pc => SET transaction_mode = 'two_pc'
	IdentifierAsString bool

	// Default is the default value, if none is given
	Default string

	Name string
}

var (
	on   = "1"
	off  = "0"
	utf8 = "'utf8'"

	Autocommit                  = SystemVariable{Name: "autocommit", IsBoolean: true, Default: on}
	ClientFoundRows             = SystemVariable{Name: "client_found_rows", IsBoolean: true, Default: off}
	SkipQueryPlanCache          = SystemVariable{Name: "skip_query_plan_cache", IsBoolean: true, Default: off}
	TxReadOnly                  = SystemVariable{Name: "tx_read_only", IsBoolean: true, Default: off}
	TransactionReadOnly         = SystemVariable{Name: "transaction_read_only", IsBoolean: true, Default: off}
	SQLSelectLimit              = SystemVariable{Name: "sql_select_limit", Default: off}
	TransactionMode             = SystemVariable{Name: "transaction_mode", IdentifierAsString: true}
	Workload                    = SystemVariable{Name: "workload", IdentifierAsString: true}
	Charset                     = SystemVariable{Name: "charset", Default: utf8, IdentifierAsString: true}
	Names                       = SystemVariable{Name: "names", Default: utf8, IdentifierAsString: true}
	SessionUUID                 = SystemVariable{Name: "session_uuid", IdentifierAsString: true}
	SessionEnableSystemSettings = SystemVariable{Name: "enable_system_settings", IsBoolean: true, Default: on}
	// Online DDL
	DDLStrategy    = SystemVariable{Name: "ddl_strategy", IdentifierAsString: true}
	Version        = SystemVariable{Name: "version"}
	VersionComment = SystemVariable{Name: "version_comment"}

	// Read After Write settings
	ReadAfterWriteGTID    = SystemVariable{Name: "read_after_write_gtid"}
	ReadAfterWriteTimeOut = SystemVariable{Name: "read_after_write_timeout"}
	SessionTrackGTIDs     = SystemVariable{Name: "session_track_gtids", IdentifierAsString: true}
)
