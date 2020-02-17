package main

import (
	"fmt"
	"time"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/dghubble/go-twitter/twitter"
)

type KafkaConsumer struct {
	Consumer *kafka.Consumer
}

func NewKafkaConsumer(brokers, topic string) KafkaConsumer {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "claws",
		"auto.offset.reset": "latest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		// TODO: handle in error channel?
		panic(err)
	}

	c.SubscribeTopics([]string{topic}, nil)
	return KafkaConsumer{c}
}


func getOffset(c *kafka.Consumer, topic string, date string) (*kafka.TopicPartition, error) {
	md, err := c.GetMetadata(&topic, false, 10000)
	if err != nil {
		return nil, nil
	}

	numPartitions := len(md.Topics["tweets"].Partitions)

	key := []byte(date)
	idx := (murmur2(key) & 0x7fffffff) % uint32(numPartitions)
	partition := int32(idx)

	_, offset, err := c.QueryWatermarkOffsets(topic, partition, 10000)
	if err != nil {
		return nil, nil
	}

	if offset > 0 {
		offset = offset - 1
	}
	return &kafka.TopicPartition{&topic, partition, kafka.Offset(offset), nil, nil}, nil
}


func getMaxID (date string) (int64, error) {
	cnf := getConfig()
	consumer := NewKafkaConsumer(cnf.KafkaBrokers, cnf.PubTopic)
	c := consumer.Consumer

	tp, err := getOffset(c, cnf.PubTopic, date)
	if err != nil {
		return 0, err
	}
	c.Assign([]kafka.TopicPartition{*tp})

	tw, err := consumer.Consume()

	if err != nil {
		return 0, err
	}

	createdAt, err := tw.CreatedAtTime()
	if err != nil {
		return 0, err
	}

	if createdAt.Format("2006-01-02") != date {
		err = fmt.Errorf("Latest tweet not the right date for recovering MaxID. Tweet was created at: %v. We are currently searching for tweets from %v", createdAt, date)
		return 0, err
	}

	return tw.ID, nil
}


func (consumer KafkaConsumer) Consume () (*twitter.Tweet, error) {
	c := consumer.Consumer

	msg, err := c.ReadMessage(30 * time.Second)

	if err != nil {
		return nil, err
	}

	val := msg.Value
	dat := new(twitter.Tweet)
	err = json.Unmarshal(val, dat)
	if err != nil {
		return dat, err
	}

	return dat, nil
}


// Taken from https://github.com/segmentio/kafka-go/blob/master/balancer.go
// Go port of the Java library's murmur2 function.
// https://github.com/apache/kafka/blob/1.0/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L353
func murmur2(data []byte) uint32 {
	length := len(data)
	const (
		seed uint32 = 0x9747b28c
		// 'm' and 'r' are mixing constants generated offline.
		// They're not really 'magic', they just happen to work well.
		m = 0x5bd1e995
		r = 24
	)

	// Initialize the hash to a random value
	h := seed ^ uint32(length)
	length4 := length / 4

	for i := 0; i < length4; i++ {
		i4 := i * 4
		k := (uint32(data[i4+0]) & 0xff) + ((uint32(data[i4+1]) & 0xff) << 8) + ((uint32(data[i4+2]) & 0xff) << 16) + ((uint32(data[i4+3]) & 0xff) << 24)
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	// Handle the last few bytes of the input array
	extra := length % 4
	if extra >= 3 {
		h ^= (uint32(data[(length & ^3)+2]) & 0xff) << 16
	}
	if extra >= 2 {
		h ^= (uint32(data[(length & ^3)+1]) & 0xff) << 8
	}
	if extra >= 1 {
		h ^= uint32(data[length & ^3]) & 0xff
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}
