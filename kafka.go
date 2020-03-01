package main

// import (
// 	"fmt"
// 	"time"
// 	"log"
// 	"encoding/json"
// 	"github.com/confluentinc/confluent-kafka-go/kafka"
//     "github.com/dghubble/go-twitter/twitter"
// )

// type KafkaConsumer struct {
// 	Consumer *kafka.Consumer
// }


// func getMaxID (date string) (int64, error) {
// 	cnf := getConfig()
// 	consumer := NewKafkaConsumer(cnf.KafkaBrokers, cnf.PubTopic)
// 	c := consumer.Consumer

// 	tp, err := getOffset(c, cnf.PubTopic, date)
// 	if err != nil {
// 		log.Printf("Reverting to 0 offset. Error getting offset: %v", err)
// 		return 0, err
// 	}

// 	c.Assign([]kafka.TopicPartition{*tp})

// 	tw, err := consumer.Consume()

// 	if err != nil {
// 		return 0, err
// 	}

// 	createdAt, err := tw.CreatedAtTime()
// 	if err != nil {
// 		return 0, err
// 	}

// 	if createdAt.Format("2006-01-02") != date {
// 		err = fmt.Errorf("Latest tweet not the right date for recovering MaxID. Tweet was created at: %v. We are currently searching for tweets from %v", createdAt, date)
// 		return 0, err
// 	}

// 	return tw.ID, nil
// }
