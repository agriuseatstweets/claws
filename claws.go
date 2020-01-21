package main

import (
	"log"
    "github.com/dghubble/go-twitter/twitter"
)

func monitor(errs <-chan error) {
	e := <- errs
	log.Fatalf("Claws failed with error: %v", e)
}

func searchAndPublish(client *twitter.Client, params twitter.SearchTweetParams, errs chan error) {
	log.Printf("Searching query: %v & Searching geocode: %v", params.Query, params.Geocode)	
	tweets := search(client, params, errs)
	messages := prepTweets(tweets, errs)
	count := publish(messages, errs)
	log.Printf("Succesfully published %v tweets.", count)	
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

func main() {
	client := getTwitterClient()	
	errs := make(chan error)
	go monitor(errs)

	params := buildParams()
	
	for _, p := range params {
		searchAndPublish(client, p, errs)
	}
}
