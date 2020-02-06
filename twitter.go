package main

import (
	"log"
	"os"
	"time"
	"encoding/json"
	"strconv"
	"net/url"
	"net/http"
    "github.com/dghubble/go-twitter/twitter"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
    "github.com/agriuseatstweets/go-pubbers/pubbers"
)


func getTwitterClient() *twitter.Client {
	// oAuth2 client better for searching
	config := &clientcredentials.Config{
		ClientID: os.Getenv("T_CONSUMER_TOKEN"),
		ClientSecret: os.Getenv("T_CONSUMER_SECRET"),
		TokenURL: "https://api.twitter.com/oauth2/token",
	}

	httpClient := config.Client(oauth2.NoContext)
	return twitter.NewClient(httpClient)
}

func ParseRateLimiting(resp *http.Response) (int, time.Duration) {
	remaining, _ := strconv.Atoi(resp.Header["X-Rate-Limit-Remaining"][0])
	reset, _ := strconv.Atoi(resp.Header["X-Rate-Limit-Reset"][0])
	untilReset := reset - int(time.Now().Unix())
	return remaining, time.Duration(untilReset) * time.Second
}

func HandleErrors(err error, httpResponse *http.Response, errs chan error) {
	switch err.(type) {
	case twitter.APIError:

		// could use err.Errors[0].Code, but this seems simpler for now
		switch httpResponse.StatusCode {

		// Twitter rate limits, so sleep until limit resets
		case 429:
			_, reset := ParseRateLimiting(httpResponse)
			log.Printf("Sleeping: %v\n", reset)
			time.Sleep(reset + time.Second)
			return

		default:
			errs <- err
			return
		}

	default:
		// HTTP Error from sling. Retry and hope connection improves.
		sleeping := 30 * time.Second
		log.Printf("HTTP Error. Sleeping %v seconds. Error: \n%v\n", sleeping, err)
		time.Sleep(sleeping)
		return
	}
}

func search(client *twitter.Client, params twitter.SearchTweetParams, errs chan error) (chan twitter.Tweet) {

	ch := make(chan twitter.Tweet)

	until, _ := time.Parse("2006-01-02", params.Until)
	log.Printf("Starting search until: %v", until)

	since := until.AddDate(0,0,-1)
	mx, err := getMaxID(since.Format("2006-01-02"))
	if err != nil {
		log.Printf("Error getting MaxID: %v", err)
	}

	params.MaxID = mx
	log.Printf("Getting tweets with MaxID: %v", mx)

	go func() {
		i := 0
		for {
			search, httpResponse, err := client.Search.Tweets(&params)

			if err != nil {
				HandleErrors(err, httpResponse, errs)
				continue
			}

			// Publish
			finished := false
			for _, tw := range search.Statuses {
				ch <- tw

				c, err := tw.CreatedAtTime()
				if err == nil && c.Before(since) {
					finished = true
				}
			}

			// Get next "max_id" to set in params
			// this is Twitter's form of pagination
			nextUrl, _ := url.Parse(search.Metadata.NextResults)
			v, ok := nextUrl.Query()["max_id"]

			// break when we run out of tweets or
			// we reach our limit
			if finished || ok == false {
				close(ch)
				break
			}

			mx, _ := strconv.ParseInt(v[0], 10, 64)
			params.MaxID = mx

			// Informational Logging
			if i % 100 == 0 {
				log.Printf("Got tweets around the time: %v", search.Statuses[0].CreatedAt)
			}
			i++
		}
	}()

	return ch
}



func prepTweets(tweets chan twitter.Tweet, errs chan error) chan pubbers.QueuedMessage {
	out := make(chan pubbers.QueuedMessage)
	go func(){
		for tw := range tweets {
			created, err := tw.CreatedAtTime()
			if err != nil {
				errs <- err
				continue
			}

			key := []byte(created.Format("2006-01-02"))
			t, err := json.Marshal(tw)

			if err != nil {
				errs <- err
				continue
			}
			out <- pubbers.QueuedMessage{key, t}
		}
		close(out)
	}()
	return out
}


func addGeocode(params twitter.SearchTweetParams, geocode string) twitter.SearchTweetParams {
	params.Geocode = geocode
	return params
}

func addQuery(params twitter.SearchTweetParams, query string) twitter.SearchTweetParams {
	params.Query = query
	return params
}
