package exmath

import (
	"math"
)

// 四舍五入函数
// f:需要四舍五入的对象
// n:需要保留小数点后几位
func Round(f float64, n int) float64 {
	pow10_n := math.Pow10(n)
	return math.Trunc((f+0.5/pow10_n)*pow10_n) / pow10_n
}
