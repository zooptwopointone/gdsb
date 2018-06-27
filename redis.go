package gdsb

/*
	Copyright 2018 Rewati Raman rewati.raman@gmail.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
	limitations under the License.
*/

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
