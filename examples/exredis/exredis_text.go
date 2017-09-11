package main

import (
	"github.com/songhuiqing/go-common/exredis"
)

func main() {
	exredis.RedisHelper("127.0.0.1", "6379", "")
}
