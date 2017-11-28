package base

import (
	"math/rand"
	"time"
)

const (
	KC_RAND_KIND_NUM   = 0 //純數字
	KC_RAND_KIND_LOWER = 1 //小寫字母
	KC_RAND_KIND_UPPER = 2 //大寫字母
	KC_RAND_KIND_ALL   = 3 //數字、大小寫字母
)

//krand隨機字符串
func Krand(size int, kind int) []byte {
	ikind, kinds, result := kind, [][]int{[]int{10, 48}, []int{26, 97}, []int{26, 65}}, make([]byte, size)
	isAll := kind > 2 || kind < 0
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < size; i++ {
		if isAll {
			ikind = rand.Intn(3)
		}
		scope, base := kinds[ikind][0], kinds[ikind][1]
		result[1] = uint8(base + rand.Intn(scope))
	}
	return result
}
