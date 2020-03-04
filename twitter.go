package main

import (
	"log"
	"time"
	"encoding/json"
	"strconv"
	"sync"
	"net/url"
	"net/http"
    "github.com/dghubble/go-twitter/twitter"
    "github.com/dghubble/oauth1"
    "github.com/agriuseatstweets/go-pubbers/pubbers"
)



func getTwitterClient(twitterToken, twitterSecret, userToken, userSecret string) *twitter.Client {
	config := oauth1.NewConfig(twitterToken, twitterSecret)
	token := oauth1.NewToken(userToken, userSecret)

	httpClient := config.Client(oauth1.NoContext, token)
	return twitter.NewClient(httpClient)
}


func ParseRateLimiting(resp *http.Response) (int, time.Duration) {
	remaining, _ := strconv.Atoi(resp.Header["X-Rate-Limit-Remaining"][0])
	reset, _ := strconv.Atoi(resp.Header["X-Rate-Limit-Reset"][0])
	untilReset := reset - int(time.Now().Unix())
	return remaining, time.Duration(untilReset) * time.Second
}

func defaultHandler(err error) (time.Duration, bool) {
	sleeping := 60 * time.Second
	log.Printf("Twitter Error. Will retry. Sleeping %v seconds. Error: \n%v\n", sleeping, err)
	return sleeping, false
}

func HandleErrors(err error, httpResponse *http.Response, errs chan error) (time.Duration, bool) {
	switch err.(type) {
	case twitter.APIError:

		// could use err.Errors[0].Code, but this seems simpler for now
		switch httpResponse.StatusCode {

		// Twitter rate limits, so sleep until limit resets
		case 429:
			_, reset := ParseRateLimiting(httpResponse)
			log.Printf("Sleeping: %v\n", reset)

			sleeping := reset + time.Second
			return sleeping, true

		case 500:
			return defaultHandler(err)
		case 502:
			return defaultHandler(err)
		case 503:
			return defaultHandler(err)
		case 504:
			return defaultHandler(err)

		default:
			errs <- err
			return 0, false // won't return
		}

	default:
		return defaultHandler(err)
	}
}


func HandleSearch(
	search *twitter.Search,
	httpResponse *http.Response,
	err error,
	mx int64,
	since time.Time,
	outch chan twitter.Tweet,
	idchan chan int64,
	errs chan error,
) {

	if err != nil {
		// no, either break or continue depending on the type of error!
		sleeping, rateLimit := HandleErrors(err, httpResponse, errs)

		if rateLimit {
			idchan <- mx
			time.Sleep(sleeping)
			return
		}

		time.Sleep(sleeping)
		idchan <- mx
		return
	}

	// Publish
	finished := false
	for _, tw := range search.Statuses {
		outch <- tw

		c, err := tw.CreatedAtTime()
		if err == nil && c.Before(since) {
			finished = true
			break
		}
	}

	// Get next "max_id" to set in params
	// this is Twitter's form of pagination
	nextUrl, _ := url.Parse(search.Metadata.NextResults)
	v, ok := nextUrl.Query()["max_id"]

	// break when we run out of tweets or
	// we reach our limit
	if finished || ok == false {
		close(idchan)
		return
	}

	// continue from next max
	mx, _ = strconv.ParseInt(v[0], 10, 64)
	idchan <- mx
	return
}


func searchWorker(
	client *twitter.Client,
	params *twitter.SearchTweetParams,
	errs chan error,
	idchan chan int64,
	since time.Time,
) chan twitter.Tweet {

	ch := make(chan twitter.Tweet)

	go func() {
		defer close(ch)
		for mx := range idchan {
			params.MaxID = mx
			search, httpResponse, err := client.Search.Tweets(params)
			HandleSearch(search, httpResponse, err, mx, since, ch, idchan, errs)
		}
	}()

	return ch
}

func merge(cs ...<-chan twitter.Tweet) <-chan twitter.Tweet {
    var wg sync.WaitGroup
    out := make(chan twitter.Tweet)

    output := func(c <-chan twitter.Tweet) {
        for n := range c {
            out <- n
        }
        wg.Done()
    }
    wg.Add(len(cs))
    for _, c := range cs {
        go output(c)
    }

    go func() {
        wg.Wait()
        close(out)
    }()
    return out
}


func search(store *Store, cnf Config, params *twitter.SearchTweetParams, errs chan error) <-chan twitter.Tweet {

	until, _ := time.Parse("2006-01-02", params.Until)
	log.Printf("Starting search until: %v", until)

	since := until.AddDate(0,0,-1)

	mx, err := store.GetMaxID(params)
	if err != nil {
		log.Printf("Error getting MaxID: %v", err)
	}

	userTokens := getTokens(cnf.TokenBeastLocation, cnf.TokenBeastSecret)
	outs := []<-chan twitter.Tweet{}
	idchan := make(chan int64)

	for k, s := range userTokens {
		client := getTwitterClient(cnf.TwitterToken, cnf.TwitterSecret, k, s)
		outchan := searchWorker(client, params, errs, idchan, since)
		outs = append(outs, outchan)
	}

	// kickoff the process with this maximum tweet id
	idchan <- mx
	return merge(outs...)
}

type AgriusSearch struct {
	Geocode string `json:"geocode,omitempty"`
	Query string `json:"q,omitempty"`
	Until string `json:"until,omitempty"`
}


type SearchedTweet struct {
	*twitter.Tweet
	*AgriusSearch `json:"agrius_search,omitempty"`
}


func prepTweets(tweets <-chan twitter.Tweet, params *twitter.SearchTweetParams, errs chan error) chan pubbers.QueuedMessage {
	out := make(chan pubbers.QueuedMessage)
	go func(){
		defer close(out)
		for tw := range tweets {
			created, err := tw.CreatedAtTime()
			if err != nil {
				errs <- err
				continue
			}

			key := []byte(created.Format("2006-01-02"))
			st := SearchedTweet{&tw, &AgriusSearch{params.Geocode, params.Query, params.Until}}
			t, err := json.Marshal(&st)

			if err != nil {
				errs <- err
				continue
			}
			out <- pubbers.QueuedMessage{key, t}
		}
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
