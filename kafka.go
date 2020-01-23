package main

import (
	"log"
	"os"
	"github.com/confluentinc/confluent-kafka-go-dev/kafka"

)

type KafkaWriter struct {
	Producer *kafka.Producer
	Topic string
}

func NewKafkaWriter() (KafkaWriter, error) {
	// get env vars

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"message.send.max.retries": "50",
		"retry.backoff.ms": "5000",
	})

	topic := os.Getenv("CLAWS_TOPIC")
	return KafkaWriter{p, topic}, err
}

func (writer KafkaWriter) Publish(messages chan QueuedTweet, errs chan error) WriteResults {

	p := writer.Producer
	topic := writer.Topic

	sent := 0
	go func(){
		for msg := range messages {

			km := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key: msg.Key,
				Value: msg.Value}

			err := p.Produce(km, nil)
			if err != nil {
				errs <- err
				break
			}
			sent++
		}

		log.Printf("Flushing outstanding messages\n")
		p.Flush(15 * 1000)
		p.Close()
	}()

	written := 0

	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				written++
			}

		case kafka.Error:
			e := ev
			if e.IsFatal() {
				errs <- e
				break
			} else {
				log.Printf("Non-fatal Error: %v\n", e)
			}
		}

	}
	return WriteResults{sent, written}

}
