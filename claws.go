package main

import (
	"log"
	"os"
    "github.com/dghubble/go-twitter/twitter"
)

func monitor(errs <-chan error) {
	e := <- errs
	log.Fatalf("Claws failed with error: %v", e)
}

func searchAndPublish(writer QueueWriter, client *twitter.Client, params twitter.SearchTweetParams, errs chan error) {
	log.Printf("Searching query: %v & Searching geocode: %v", params.Query, params.Geocode)
	tweets := search(client, params, errs)
	messages := prepTweets(tweets, errs)

	results := writer.Publish(messages, errs)

	log.Printf("Succesfully published %v tweets out of %v sent", results.Written, results.Sent)
}

func buildParams() []twitter.SearchTweetParams {
	var paramsList []twitter.SearchTweetParams

	params := twitter.SearchTweetParams{
		Count: 100,
		TweetMode: "extended",
	}

	locations := getLocations()
	for _, loc := range locations {
		paramsList = append(paramsList, addGeocode(params, loc))
	}

	// TODO: add support for hastags/users/urls
	// add them to paramsList

	return paramsList
}

func getWriter() (QueueWriter, error) {
	writer := os.Getenv("CLAWS_QUEUE")
	switch writer {
	case "kafka":
		return NewKafkaWriter()
	case "pubsub":
		return NewPubSubWriter()
	default:
		panic("Please provide a valid queue!")
	}
}

func main() {
	client := getTwitterClient()
	writer, err := getWriter()
	if err != nil {
		log.Fatal(err)
	}

	errs := make(chan error)
	go monitor(errs)

	params := buildParams()

	for _, p := range params {
		searchAndPublish(writer, client, p, errs)
	}
}
