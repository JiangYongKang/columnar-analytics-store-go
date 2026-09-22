package colstore

import "math"

// 独立小文件隔离浮点位转换，便于复用与测试。
func floatToBits(f float64) uint64 { return math.Float64bits(f) }
func bitsToFloat(b uint64) float64 { return math.Float64frombits(b) }
