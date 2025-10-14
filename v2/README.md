# 🪄 Mysqlbinlog

A Go library for efficient database rollback using MySQL binary logs.






# 🚀 Overview

Mysqlbinlog is a lightweight Go library that lets you undo database changes by reading MySQL's binary logs.
It’s especially useful in automated testing, data recovery, or sandboxed environments where you need to repeatedly reset the database state.

# ✨ Features

🔄 Real-time tracking of database changes via MySQL binlog

🧾 Automatic generation of rollback SQL statements

⚡ Delta-based rollback for high efficiency

🧩 Supports INSERT, UPDATE, and DELETE operations

🧠 Optional schema caching for faster startup

# 📋 Requirements

- MySQL server with binlog enabled
- MySQL user with privileges: REPLICATION CLIENT, SELECT
- Go 1.18+

# 📦 Installation
```
go get github.com/zhangqibupt/mysqlbinlog/v2
```

# 🧠 Quick Start

Here’s a simple example of resetting database state between test cases:

```
package main

import (
    "log"
    "github.com/zhangqibupt/mysqlbinlog/v2"
)

func main() {
    // 1️⃣ Initialize the binlog listener
    err := mysqlbinlog.Start("localhost", 3306, "user", "password")
    if err != nil {
        log.Fatalf("Failed to start binlog listener: %v", err)
    }
    defer mysqlbinlog.Stop()

    // 2️⃣ Example: run multiple test cases with rollback
    for i := 0; i < 10; i++ {
        mysqlbinlog.Begin() // start tracking

        // ... perform DB operations: INSERT, UPDATE, DELETE

        mysqlbinlog.Rollback() // undo all changes
    }
}
```


✅ Tip: Schema caching can drastically improve startup time in local development (see config below).

## 🔧 Environment Variables

| Variable | Description |
|-----------|--------------|
| **MYSQL_BINLOG_CACHE** | Enable schema caching (stores cache files locally: `mysql.binlog.cache.1`, `mysql.binlog.cache.2`) |
| **MYSQL_BINLOG_LOG_LEVEL** | Logging verbosity. Options: `debug`, `info`, `warn`, `error` (default: `info`) <br> Use `debug` to print generated rollback SQLs |

---

## ⚠️ Limitations

| Category | Description |
|-----------|--------------|
| **DDL Operations** | Schema-altering statements (`CREATE`, `ALTER`, `DROP`) are **not supported**. <br> Changing table structures during runtime may cause errors. |
| **Concurrency** | Running tests in parallel is **not supported** — rollback statements might interleave. |
| **Performance** | Schema loading can take several seconds initially. <br> Use caching to speed up development. |


# 💡 Best Practices

- Always call Stop() before program exit.
- Use Begin() / Rollback() in pairs.
- Enable schema caching during development.
- Set MYSQL_BINLOG_LOG_LEVEL=debug to inspect rollback SQL.
- Make sure MySQL binlog is properly configured (binlog_format=ROW recommended).

# 🧩 Example

👉 See `example/main.go` for a complete working demo.

# 🤝 Contributing

Contributions are welcome!
Please open an issue or submit a PR to help improve the library.

# 📄 License

MIT License
 © 2025 Zhang Qi