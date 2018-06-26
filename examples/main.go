package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis"
	"github.com/rewati/gdsb"
)

var c = struct {
	Appname             string
	Cassandra           gdsb.CassandraConfig
	KafkaProducerConfig gdsb.KafkaConf
	KafkaConsumerConfig gdsb.KafkaConf
	RedisConfiguration  gdsb.RedisConfig
}{}

var sent = 0
var received = 0

type Ast struct {
	ID  string `json:"messageId"`
	DID string `json:"messageDId"`
}

func main() {
	g := []struct {
		Name string
		Age  int
		Dept string
	}{
		{Name: "rt", Age: 45},
		{Name: "rt1", Age: 456, Dept: "HR"},
	}
	var configuration = gdsb.Configuration{Config: &c}
	if err := gdsb.LoadConfigurations(configuration); err != nil {
		log.Panic("Error in configuration")
	}
	ast := Ast{ID: "id123", DID: "did123"}
	b, e1 := gdsb.ToJSON(&ast)
	fmt.Printf("%v | %v\n", string(b), e1)
	var a Ast
	json.Unmarshal([]byte(string(b)), &a)
	fmt.Printf("AST: %v ", a)
	redisExample()
	fmt.Printf("Cassandra config: %v", c.Cassandra)
	go kafkaConsumerExample()
	kafkaExample()
	run := true
	for run == true {
		sent++
		if sent%10 == 0 {
			log.Println("Total sent: ", sent)
		}
		if sent == 1000 {
			run = false
		}
		time.Sleep(1 * time.Second)
		gdsb.Produce(gdsb.KfMsg{Topic: "rewati_test", Object: g, Key: gdsb.UUIDstring()})
	}
	gdsb.Hold()
}

func kafkaExample() {
	//Initialization. Required one time during application startup.
	//Can be done anywhere in application
	if err := gdsb.InitKafkaProducer(c.KafkaProducerConfig); err != nil {
		log.Panicf("Kafka initialization error: %v", err)
	}

	//This is to produce message to a topic.
	//Can be done from anywhere in application
	gdsb.Produce(gdsb.KfMsg{Topic: "rewati_test", Message: "Hello"})
}

func kafkaConsumerExample() {
	ch := make(chan gdsb.KafkaMessage)
	if err := gdsb.CreateKafkaSubscription(c.KafkaConsumerConfig, []string{"rewati_test"}, ch); err != nil {
		log.Printf("Error kafka consume: %v", err)
	}
	for msg := range ch {
		log.Printf("%v\n", msg)
		if received == 1000 {
			panic(" done")
		}
		if received%100 == 0 {
			log.Println("Total received: ", sent)
		}
	}
}

func cassandraExample() {
	//Initialization. Required one time during application startup.
	//Can be done anywhere in application
	if err := gdsb.CassandraSessionInit(c.Cassandra); err != nil {
		log.Panicf("Cassandra initialization error: %v", err)
	}

	//This is to run update or insert cassandra query.
	//Can be done from anywhere in application
	q := gdsb.CassandraQuery{Querry: "update Message_Counters set urlviewcount=urlviewcount+1 where miniurl='loadtest1';"}
	if err := gdsb.CQUpsert(q); err != nil {
		log.Println("Cassandra execution error: ", err)
	}
}

func redisExample() {
	gdsb.LoadRedisClient(c.RedisConfiguration)
	println(">>>>>>>>> ", accountUsersOnline("jfnibfv"))
	sendToRedis()
}

func sendToRedis() {
	ast := Ast{ID: gdsb.UUIDstring(), DID: "did123"}
	b, _ := gdsb.ToJSON(&ast)
	cmd := gdsb.RedisClient.ZAdd("zsd", redis.Z{Member: b, Score: float64(gdsb.UTCMilisec())})
	time.Sleep(1 * time.Second)
	if cmd.Err() != nil {
		fmt.Println(cmd.Err())
	}
	sendToRedis()
}

func accountUsersOnline(accountSid string) bool {
	cmd := gdsb.RedisClient.Get(accountSid)
	err := cmd.Err()
	if err != nil {
		log.Printf("Unable to get account exist info from redis, for Account: %v Error: %v\n", accountSid, err)
		return false
	}
	if cmd.Val() != "" {
		return true
	}
	return false
}
