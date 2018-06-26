package gdsb

import (
	"strings"

	"github.com/go-redis/redis"
)

//RedisClient is redis client
var RedisClient *redis.Client

//RedisConfig holds configuration for redisclient creation.
type RedisConfig struct {
	ClientType string
	Master     string
	Hosts      string
	Password   string
	DB         int
}

//CreateClient will create normal client
func CreateClient(config RedisConfig) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Hosts,
		Password: config.Password,
		DB:       config.DB,
	})
	return client
}

//CreateSentinelConnection for creating Sentinel connection
func CreateSentinelConnection(config RedisConfig) *redis.Client {
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    config.Master,
		SentinelAddrs: strings.Split(config.Hosts, ","),
	})
	return client
}

//CreateRedisClient creates redis client
func CreateRedisClient(config RedisConfig) *redis.Client {
	if strings.ToLower(config.ClientType) == "sentinel" {
		return CreateSentinelConnection(config)
	}
	return CreateClient(config)
}

//LoadRedisClient load redis client configuration
func LoadRedisClient(config RedisConfig) {
	RedisClient = CreateRedisClient(config)
}
