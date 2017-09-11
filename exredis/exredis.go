package exredis

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// 根据redis ip地址，端口和访问密码获取redis连接对象
func RedisHelper(ip string, port string, password string) redis.Conn {
	c, err := redis.Dial("tcp", ip+":"+port)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if _, err := c.Do("AUTH", password); err != nil {
		c.Close()
		return nil
	}
	return c
}

// redis set方法
func Set(c redis.Conn, key string, value interface{}) interface{} {
	v, err := c.Do("set", key, value)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return v
}

// get方法
func Get(c redis.Conn, key string) interface{} {
	v, err := redis.String(c.Do("get", key))
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return v
}

func Lpush(c redis.Conn, qname string, value string) {
	c.Do("lpush", qname, value)
}

func Del(c redis.Conn, key string) {
	c.Do("del", key)
}

func Llen(c redis.Conn, qname string) interface{} {
	v, err := c.Do("llen", qname)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return v
}

func Exists(c redis.Conn, key string) interface{} {
	v, err := c.Do("exists", key)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return v
}

func Rpop(c redis.Conn, qname string) string {
	v, err := redis.String(c.Do("rpop", qname))
	if err != nil {
		// fmt.Println(err)
		return ""
	}
	return v
}
