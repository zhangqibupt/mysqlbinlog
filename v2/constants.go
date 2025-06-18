package mysqlbinlog

const (
	CUnknowncolprefix = "dropped_column_"
	CUnknowncoltype   = "unknown_type"

	CReprocess  = 0
	CRecontinue = 1
	CRebreak    = 2
	CRefileend  = 3
)

const (
	disableBinlogSQL    = "SET sql_log_bin = OFF;"
	disableKeyCheckSQL  = "SET FOREIGN_KEY_CHECKS=0;"
	showMasterStatusSQL = "SHOW MASTER STATUS;"
)

const (
	primaryUniqueKeysSQL = `
		select k.table_schema, k.table_name, k.CONSTRAINT_NAME, k.COLUMN_NAME, c.CONSTRAINT_TYPE, k.ORDINAL_POSITION
		from information_schema.TABLE_CONSTRAINTS as c inner join information_schema.KEY_COLUMN_USAGE as k on
		c.CONSTRAINT_NAME = k.CONSTRAINT_NAME and c.table_schema = k.table_schema and c.table_name=k.table_name
		where c.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE') and c.table_schema ='%s' and c.table_name in (%s)
		order by k.table_schema asc, k.table_name asc, k.CONSTRAINT_NAME asc, k.ORDINAL_POSITION asc
	`

	columnNamesTypesSQL = `
		select table_schema, table_name, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, EXTRA from information_schema.columns
		where table_schema ='%s' and table_name in (%s)
		order by table_schema asc, table_name asc, ORDINAL_POSITION asc
	`

	autoIncrementsSQL   = "SELECT `TABLE_SCHEMA`, `TABLE_NAME`, `AUTO_INCREMENT` FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME IN (%s)"
	setAutoIncrementSQL = "ALTER TABLE %s.%s AUTO_INCREMENT=%d"
)

var tableinfo tablesColumnsInfo

const getTableNamesSQL = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('information_schema', 'performance_schema');"

type SQLType byte

const (
	SQLTypeInsert SQLType = iota
	SQLTypeUpdate
	SQLTypeDelete
	SQLTypeQuery
)

var bytesColumnTypes = []string{"blob", "json", "geometry", CUnknowncoltype}

const BLOB = "blob"
