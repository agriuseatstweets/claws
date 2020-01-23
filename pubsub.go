package main

import (
	"log"
	"os"
	"context"
	"cloud.google.com/go/pubsub"
)

type PubSubWriter struct {
	Topic *pubsub.Topic
	Ctx context.Context
}

func NewPubSubWriter() (PubSubWriter, error){
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, os.Getenv("GOOGLE_PROJECT_ID"))

	if err != nil {
		return PubSubWriter{nil, ctx}, err
	}

	name := os.Getenv("CLAWS_TOPIC")
	log.Printf("Publishing to topic: %v\n", name)
	topic := client.Topic(name)
	return PubSubWriter{topic, ctx}, nil
}

func (writer PubSubWriter) Publish(messages chan QueuedTweet, errs chan error) WriteResults {
	results := make(chan *pubsub.PublishResult)

	topic := writer.Topic
	ctx := writer.Ctx

	defer topic.Stop()

	sent := 0
	go func(){
		for msg := range messages {
			data := msg.Value
			attrs := map[string]string{ "id": string(msg.Key) }
			r := topic.Publish(ctx, &pubsub.Message{
				Data: data,
				Attributes: attrs,
			})
			results <- r
			sent++
		}
		close(results)
	}()

	written := 0
	for r := range results {
		// .Get blocks until response received
		_, err := r.Get(ctx)
		if err != nil {
			errs <- err
		}
		written++
	}

	return WriteResults{sent, written}
}
