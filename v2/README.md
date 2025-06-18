# Mysqlbinlog

A Go library for efficient database rollback operations using MySQL binlog.

## Overview

Mysqlbinlog provides a lightweight solution for rolling back database changes by leveraging MySQL's binary logging mechanism. It's particularly useful in scenarios like regression testing where you need to reset database state between test cases.

### Key Features

- Real-time monitoring of database changes through MySQL binlog
- Automatic generation of rollback SQL statements
- Efficient delta-based rollback operations
- Support for INSERT, UPDATE, and DELETE operations

## Prerequisites

- MySQL server with binlog enabled
- Go 1.18 or later
- MySQL user with appropriate privileges (REPLICATION CLIENT, SELECT)

## Installation

```bash
go get github.freewheel.tv/bricks/mysqlbinlog
```

## Quick Start

```go
import "github.freewheel.tv/bricks/mysqlbinlog"

func main() {
    // Initialize the binlog listener
    err := mysqlbinlog.Start("localhost", 3306, "user", "password")
    if err != nil {
        log.Fatalf("Failed to start binlog listener: %v", err)
    }
    defer mysqlbinlog.Stop()

    // Run your test cases
    for i := 0; i < 10; i++ {
        // Mark the beginning of a test case
        mysqlbinlog.Begin()
        
        // Execute your test operations
        // INSERT, UPDATE, DELETE statements...
        
        // Rollback changes after the test
        mysqlbinlog.Rollback()
    }
}
```

## API Reference

### Core Functions

- `Start(host string, port uint, user string, password string) error`
  - Initializes the binlog listener
  - Connects to the MySQL server
  - Loads table schemas

- `Begin()`
  - Marks the beginning of a new operation set
  - Used to track changes for rollback

- `Rollback()`
  - Reverts all changes made since the last `Begin()`
  - Executes generated rollback SQL statements

- `Stop()`
  - Stops the binlog listener
  - Cleans up resources

### Configuration

#### Environment Variables

- `MYSQL_BINLOG_CACHE`: Set to any value to enable schema caching
  - Caches table schemas in the current working directory
  - Improves startup time for local testing
  - Cache files: `mysql.binlog.cache.1` and `mysql.binlog.cache.2`

- `MYSQL_BINLOG_LOG_LEVEL`: Set logging level
  - Default: "info"
  - Options: "debug", "info", "warn", "error"
  - Debug level prints rollback SQL statements

## Limitations

1. DDL Operations
   - Schema changes (CREATE, ALTER, DROP) are not supported
   - Table structure changes during operation may cause issues

2. Concurrency
   - Parallel test execution is not supported
   - Rollback SQLs may be mixed up if multiple cases run simultaneously

3. Performance
   - Initial schema loading may take several seconds
   - Use `MYSQL_BINLOG_CACHE` for faster local development

## Best Practices

1. Always call `Stop()` before program termination
2. Use `Begin()` and `Rollback()` in pairs
3. Enable schema caching for local development
4. Set appropriate log level for debugging
5. Ensure MySQL binlog is enabled and properly configured

## Example

See the [example](./example/main.go) directory for a complete working example.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT
