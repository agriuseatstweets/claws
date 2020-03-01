package main

import (
	"log"
	"time"
    "github.com/dghubble/go-twitter/twitter"
	"github.com/caarlos0/env/v6"
    "github.com/agriuseatstweets/go-pubbers/pubbers"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func monitor(errs <-chan error) {
	e := <- errs
	log.Fatalf("Claws failed with error: %v", e)
}


func logprog(store *Store, params *twitter.SearchTweetParams, messages <-chan *kafka.Message) <-chan []byte {
	ch := make(chan []byte)

	go func(){
		defer close(ch)
		i := 0
		for m := range messages {
			ch <- m.Key
			if i % 1000 == 0 {

				tweet := new(twitter.Tweet)
				if err := json.Unmarshal(m.Value, tweet); err != nil {
					continue
				}

				// Checkpoint every 1k tweets
				store.SetMaxID(params, tweet.ID)

				// Log every 18k tweets
				if i % 18000 == 0 {
					t, _ := tweet.CreatedAtTime()
					log.Printf("Got tweet from date: %v", t)
				}
			}
			i++
		}
	}()

	return ch
}


func searchAndPublish(store *Store, cnf Config, params *twitter.SearchTweetParams, errs chan error) {
	log.Printf("Searching query: %v & Searching geocode: %v", params.Query, params.Geocode)

	writer, err := getWriter(cnf.KafkaBrokers, cnf.PubTopic)
	if err != nil {
		errs <- err
	}

	tweets := search(store, cnf, params, errs)
	messages := prepTweets(tweets, params, errs)
	outs, results := writer.Publish(messages, errs)
	keys := logprog(store, params, outs)

	written := 0
	for _ = range keys {
		written++
	}

	log.Printf("Succesfully published %v tweets out of %v sent", written, results.Sent)
}


func buildParams(until time.Time) []twitter.SearchTweetParams {
	var paramsList []twitter.SearchTweetParams

	params := twitter.SearchTweetParams{
		Count: 100,
		TweetMode: "extended",
		Until: until.Format("2006-01-02"),
	}

	locations := getLocations()
	for _, loc := range locations {
		paramsList = append(paramsList, addGeocode(params, loc))
	}

	// TODO: add support for hastags/users/urls
	// add them to paramsList

	return paramsList
}

func getWriter(brokers, topic string) (pubbers.KafkaWriter, error) {
	wc := pubbers.KafkaWriterConfig{brokers, topic}
	return pubbers.NewKafkaWriter(wc)
}


func buildUntil(days int) time.Time {
	t := time.Now()
	t = t.AddDate(0,0,days)
	return t
}

func digit(cnf Config, date time.Time, errs chan error) {
	digs := make(chan pubbers.QueuedMessage)

	go func() {
		defer close(digs)

		ti := date.Format("2006-01-02")
		dig := pubbers.QueuedMessage{[]byte(ti), []byte(ti)}
		digs <- dig
	}()

	writer, err := getWriter(cnf.KafkaBrokers, cnf.DigTopic)
	if err != nil {
		errs <- err
	}

	writer.Publish(digs, errs)
	log.Printf("Succesfully published dig report for %v", date)
}


type Config struct {
	KafkaBrokers string `env:"KAFKA_BROKERS,required"`
	PubTopic string `env:"PUB_TOPIC,required"`
	DigTopic string `env:"DIG_TOPIC,required"`
	Queue string `env:"CLAWS_QUEUE,required"`
	TwitterToken string `env:"T_CONSUMER_TOKEN,required"`
	TwitterSecret string `env:"T_CONSUMER_SECRET,required"`
	TokenBeastLocation string `env:"BEAST_LOCATION,required"`
	TokenBeastSecret string `env:"BEAST_SECRET,required"`
	RedisHost string `env:"REDIS_HOST,required"`
}

func getConfig() Config {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		panic(err)
	}
	return cfg
}

func main() {
	cnf := getConfig()
	store := GetStore(cnf.RedisHost)

	errs := make(chan error)
	go monitor(errs)

	until := buildUntil(-5)
	params := buildParams(until)

	for _, p := range params {
		searchAndPublish(store, cnf, &p, errs)
	}

	digit(cnf, until.AddDate(0,0,-1), errs)
}
