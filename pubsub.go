package main

import (
	"log"
	"os"
	"context"
	"cloud.google.com/go/pubsub"
)


func getPubSub() (*pubsub.Topic, context.Context, error){
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, os.Getenv("GOOGLE_PROJECT_ID"))

	if err != nil {
		return nil, ctx, err
	}

	name := os.Getenv("CLAWS_TOPIC")
	log.Printf("Publishing to topic: %v\n", name)
	topic := client.Topic(name)
	return topic, ctx, nil
}

func publish(messages chan []byte, errs chan error) int {
	results := make(chan *pubsub.PublishResult)
	topic, ctx, err := getPubSub()

	if err != nil {
		errs <- err
		return 0
	}

	defer topic.Stop()

	go func(){
		for msg := range messages {
			r := topic.Publish(ctx, &pubsub.Message{
				Data: msg,
			})
			results <- r
		}
		close(results)
	}()

	count := 0

	for r := range results {
		count++
		
		// .Get blocks until response received
		_, err := r.Get(ctx)
		if err != nil {
			errs <- err
		}
	}

	return count
}

