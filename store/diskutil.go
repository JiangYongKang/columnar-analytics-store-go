package store

import (
	"os"
)

const reservedKeyPrefix = "$comp-"

func isReservedKey(key string) bool {
	return len(key) >= len(reservedKeyPrefix) && key[:len(reservedKeyPrefix)] == reservedKeyPrefix
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// 本文件保留 isReservedKey/fileSize 等小型磁盘工具。
