package store

import "os"

// removeFile 删除文件；文件不存在视为成功。
func removeFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
