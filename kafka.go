package gdsb

/*
	Copyright 2018 Rewati Raman rewati.raman@gmail.com https://github.com/rewati/gdsb

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
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var producer *kafka.Producer
var keyEnabled bool
var keyMap = make(map[string]int)
var keylistInWindow = list.New()
var windowSize = 1000

//KafkaConf Kafka configuration
type KafkaConf struct {
	Config     []string
	KeyEnabled bool
}

//KafkaMessage is wrapper of kafka messages
type KafkaMessage struct {
	Offset         kafka.Offset
	Message        string
	MessageBytes   []byte
	PartitionIndex int32
	Key            string
}

//KfMsg is new kafka message that need to be inserted
type KfMsg struct {
	Topic   string
	Key     string
	Message string
	Object  interface{}
}

//InitKafkaProducer  creates Kaka Producer
func InitKafkaProducer(conf KafkaConf) error {
	p, err := CreateKafkaProducer(conf)
	if err == nil {
		go startHandleingFailures()
		producer = p
	}
	if conf.KeyEnabled == true {
		keyEnabled = true
	}
	return err
}

//CreateKafkaProducer  creates Kaka Producer
func CreateKafkaProducer(conf KafkaConf) (*kafka.Producer, error) {
	v := conf.toConfigMap()
	p, err := kafka.NewProducer(&v)
	if err == nil {
		go startHandleingFailures()
	}
	return p, err
}

//Produce will produce to kafka topic with if key passed.
//if the key passed  is empty random uuid will be created
func Produce(msg KfMsg) error {
	v, e := kfkMsgToByteArr(msg)
	if e != nil {
		return e
	}
	var mk = msg.Key
	if keyEnabled == true && len(msg.Key) == 0 {
		mk = UUIDstring()
	}
	return producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &msg.Topic, Partition: kafka.PartitionAny},
		Value:          v,
		Key:            []byte(mk),
	}, nil)
}

func kfkMsgToByteArr(msg KfMsg) ([]byte, error) {
	var v []byte
	if msg.Message != "" {
		v = []byte(msg.Message)
		return v, nil
	}
	if msg.Object != nil {
		return json.Marshal(msg.Object)
	}
	return v, errors.New("no payload found to send to kafka")
}

// Delivery report handler for produced messages
func startHandleingFailures() {
	for e := range producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				log.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		}
	}
}

//CreateKafkaSubscription will create kafka consumer and startsubcripton to a channel
func CreateKafkaSubscription(conf KafkaConf, topics []string, ch chan KafkaMessage) error {
	c, err := CreateKafkaConsumer(conf)
	if err != nil {
		return err
	}
	e := c.SubscribeTopics(topics, nil)
	go startSubscription(c, topics, ch)
	return e
}

//CreateKafkaConsumer creates kafka consumers
func CreateKafkaConsumer(conf KafkaConf) (*kafka.Consumer, error) {
	v := conf.toConfigMap()
	v.SetKey("go.application.rebalance.enable", true)
	v.SetKey("go.events.channel.enable", true)
	consumer, err := kafka.NewConsumer(&v)
	return consumer, err
}

func startSubscription(c *kafka.Consumer, topics []string, ch chan KafkaMessage) error {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
			case *kafka.Message:
				handleKafkaMessage(e, ch)
			case kafka.PartitionEOF:
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}
}

func handleKafkaMessage(e *kafka.Message, ch chan KafkaMessage) {
	m := KafkaMessage{
		Offset:         e.TopicPartition.Offset,
		Key:            string(e.Key),
		Message:        string(e.Value),
		MessageBytes:   e.Value,
		PartitionIndex: e.TopicPartition.Partition}
	if alreadyProcessed(m.Key) == false {
		ch <- m
	}
}

func alreadyProcessed(key string) bool {
	defer slideWindow(key)
	if k := keyMap[key]; k != 0 {
		return true
	}
	return false
}

func (c KafkaConf) toConfigMap() kafka.ConfigMap {
	k := kafka.ConfigMap{}
	for _, v := range c.Config {
		k.Set(v)
	}
	return k
}

func slideWindow(key string) {
	keylistInWindow.PushBack(key)
	if keylistInWindow.Len() > windowSize {
		j := keylistInWindow.Front().Value.(string)
		keylistInWindow.Remove(keylistInWindow.Front())
		delete(keyMap, j)
	}
	keyMap[key] = 1
}
