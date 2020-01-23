package main

import (
    "github.com/dghubble/go-twitter/twitter"
	"encoding/json"
)

type WriteResults struct {
	Sent int
	Written int
}

type QueueWriter interface {
	Publish (chan QueuedTweet, chan error) WriteResults
}

type QueuedTweet struct {
	Key []byte
	Value []byte
}


func prepTweets(tweets chan twitter.Tweet, errs chan error) chan QueuedTweet {
	out := make(chan QueuedTweet)
	go func(){
		for tw := range tweets {
			id := []byte(tw.IDStr)
			t, err := json.Marshal(tw)

			if err != nil {
				errs <- err
				continue
			}
			out <- QueuedTweet{id, t}
		}
		close(out)
	}()
	return out
}
