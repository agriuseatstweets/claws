package main

import (
	"log"
	"os"
	"time"
    "github.com/dghubble/go-twitter/twitter"
    "github.com/agriuseatstweets/go-pubbers/pubbers"
)

func monitor(errs <-chan error) {
	e := <- errs
	log.Fatalf("Claws failed with error: %v", e)
}

func searchAndPublish(writer pubbers.QueueWriter, client *twitter.Client, params twitter.SearchTweetParams, errs chan error) {
	log.Printf("Searching query: %v & Searching geocode: %v", params.Query, params.Geocode)
	tweets := search(client, params, errs)
	messages := prepTweets(tweets, errs)
	results := writer.Publish(messages, errs)

	log.Printf("Succesfully published %v tweets out of %v sent", results.Written, results.Sent)
}

func buildUntil(days int) string {
	t := time.Now()
	t = t.AddDate(0,0,days)
	return t.Format("2006-01-02")
}

func buildParams() []twitter.SearchTweetParams {
	var paramsList []twitter.SearchTweetParams

	params := twitter.SearchTweetParams{
		Count: 100,
		TweetMode: "extended",
		Until: buildUntil(-5), // until 5 days ago...
	}

	locations := getLocations()
	for _, loc := range locations {
		paramsList = append(paramsList, addGeocode(params, loc))
	}

	// TODO: add support for hastags/users/urls
	// add them to paramsList

	return paramsList
}

func getWriter() (pubbers.QueueWriter, error) {
	writer := os.Getenv("CLAWS_QUEUE")
	switch writer {
	case "kafka":
		return pubbers.NewKafkaWriter()
	case "pubsub":
		return pubbers.NewPubSubWriter()
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
