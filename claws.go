package main

import (
	"log"
	"time"
    "github.com/dghubble/go-twitter/twitter"
	"github.com/caarlos0/env/v6"
    "github.com/agriuseatstweets/go-pubbers/pubbers"
)

func monitor(errs <-chan error) {
	e := <- errs
	log.Fatalf("Claws failed with error: %v", e)
}

func searchAndPublish(writer pubbers.QueueWriter, cnf Config, params *twitter.SearchTweetParams, errs chan error) {
	log.Printf("Searching query: %v & Searching geocode: %v", params.Query, params.Geocode)
	tweets := search(cnf, params, errs)
	messages := prepTweets(tweets, params, errs)
	results := writer.Publish(messages, errs)

	log.Printf("Succesfully published %v tweets out of %v sent", results.Written, results.Sent)
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

func getWriter(brokers, topic string) (pubbers.QueueWriter, error) {
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
	writer, err := getWriter(cnf.KafkaBrokers, cnf.PubTopic)

	if err != nil {
		log.Fatal(err)
	}

	errs := make(chan error)
	go monitor(errs)

	until := buildUntil(-5)
	params := buildParams(until)

	for _, p := range params {
		searchAndPublish(writer, cnf, &p, errs)
	}

	digit(cnf, until.AddDate(0,0,-1), errs)
}
