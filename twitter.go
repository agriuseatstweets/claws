package main

import (
	"log"
	"os"
	"time"
	"strconv"
	"net/url"
	"net/http"
    "github.com/dghubble/go-twitter/twitter"
    "github.com/dghubble/oauth1"
)

func parseRateLimiting(resp *http.Response) (int, time.Duration) {
	remaining, _ := strconv.Atoi(resp.Header["X-Rate-Limit-Remaining"][0])
	reset, _ := strconv.Atoi(resp.Header["X-Rate-Limit-Reset"][0])
	untilReset := reset - int(time.Now().Unix())
	return remaining, time.Duration(untilReset) * time.Second
}

func search(client *twitter.Client, params twitter.SearchTweetParams, errs chan error) (chan twitter.Tweet) {

	ch := make(chan twitter.Tweet)

	go func() {
		for {
			search, httpResponse, err := client.Search.Tweets(&params)

			if err != nil {
				switch httpResponse.StatusCode {

				// Twitter rate limits, so sleep until limit resets
				case 429:
					_, reset := parseRateLimiting(httpResponse)
					log.Printf("Sleeping: %v\n", reset)
					time.Sleep(reset + time.Second)
					continue

				default:
					errs <- err
					continue // return or continue here?
				}
			}

			for _, tw := range search.Statuses {
				ch <- tw
			}

			nextUrl, _ := url.Parse(search.Metadata.NextResults)

			v, ok := nextUrl.Query()["max_id"]
			if ok == false {
				close(ch)
				break
			}

			mx, _ := strconv.ParseInt(v[0], 10, 64)
			params.MaxID = mx
		}
	}()

	return ch
}

func getTwitterClient() *twitter.Client {
	config := oauth1.NewConfig(
		os.Getenv("T_CONSUMER_TOKEN"),
		os.Getenv("T_CONSUMER_SECRET"))

	token := oauth1.NewToken(
		os.Getenv("T_ACCESS_TOKEN"),
		os.Getenv("T_TOKEN_SECRET"))
	httpClient := config.Client(oauth1.NoContext, token)

	return twitter.NewClient(httpClient)
}

func addGeocode(params twitter.SearchTweetParams, geocode string) twitter.SearchTweetParams {
	params.Geocode = geocode
	return params
}

func addQuery(params twitter.SearchTweetParams, query string) twitter.SearchTweetParams {
	params.Query = query
	return params
}
