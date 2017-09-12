package exredis

import (
	"github.com/garyburd/redigo/redis"
)

// 根据redis ip地址，端口和访问密码获取redis连接对象
func RedisHelper(ip string, port string, password string) (redis.Conn, error) {
	if port == "" {
		port = "6379"
	}
	c, err := redis.Dial("tcp", ip+":"+port)
	if err != nil {
		// fmt.Println(err)
		return nil, err
	}
	if password != "" {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, err
}

func Set(c redis.Conn, key string, value interface{}) (interface{}, error) {
	v, err := c.Do("set", key, value)
	if err != nil {
		// fmt.Println(err)
		return nil, err
	}
	return v, err
}

func Get(c redis.Conn, key string) (interface{}, error) {
	v, err := redis.String(c.Do("get", key))
	if err != nil {
		// fmt.Println(err)
		return nil, err
	}
	return v, err
}

func Lpush(c redis.Conn, qname string, value string) {
	c.Do("lpush", qname, value)
}

func Del(c redis.Conn, key string) {
	c.Do("del", key)
}

func Llen(c redis.Conn, qname string) (interface{}, error) {
	v, err := c.Do("llen", qname)
	if err != nil {
		// fmt.Println(err)
		return nil, err
	}
	return v, err
}

func Exists(c redis.Conn, key string) (interface{}, error) {
	v, err := c.Do("exists", key)
	if err != nil {
		// fmt.Println(err)
		return nil, err
	}
	return v, err
}

func Rpop(c redis.Conn, qname string) (string, error) {
	v, err := redis.String(c.Do("rpop", qname))
	if err != nil {
		// fmt.Println(err)
		return "", err
	}
	return v, err
}
