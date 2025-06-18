package mysqlbinlog

import (
	"github.com/sirupsen/logrus"
	"os"
)

func init() {
	// Read log level from environment variable
	logLevel := os.Getenv("MYSQL_BINLOG_LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info" // Default to info
	}

	// Parse and set the log level
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logrus.Warnf("Invalid log level '%s', defaulting to info", logLevel)
		level = logrus.InfoLevel
	}
	logrus.SetLevel(level)

	// Set log formatter
	logrus.SetFormatter(&logrus.TextFormatter{})

	logrus.Infof("mysqlbinlog package initialized with log level: %s", level)
}
