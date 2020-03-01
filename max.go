package main

import (
	"github.com/mediocregopher/radix/v3"
	"github.com/dgryski/go-farm"
    "github.com/dghubble/go-twitter/twitter"
	"fmt"
	"log"
	"strconv"
	"encoding/json"
)


type Store struct {
	client *radix.Pool
}

func GetStore(host string) *Store {
	pool, err := radix.NewPool("tcp", host, 10)

	if err != nil {
		panic(err)
	}

	return &Store{pool}
}

func keyName (date string, search uint64) string {
	return fmt.Sprintf("%v:%v", date, search)
}

func (s *Store) GetMaxID (params *twitter.SearchTweetParams) (int64, error) {
	search := searchHash(params)

	var val int64
	err := s.client.Do(radix.Cmd(&val, "GET", keyName(params.Until, search)))

	log.Printf("Got MaxID of %v", val)
	return val, err
}

func (s *Store) SetMaxID (params *twitter.SearchTweetParams, mx int64) error {
	search := searchHash(params)

	val := strconv.FormatInt(mx, 10)
	err := s.client.Do(radix.Cmd(nil, "SET", keyName(params.Until, search), val))

	if err != nil {
		return err
	}

	err = s.client.Do(radix.Cmd(nil, "EXPIRE", keyName(params.Until, search), "172800"))
	return err
}

func searchHash(params *twitter.SearchTweetParams) uint64 {
	search := &AgriusSearch{params.Geocode, params.Query, params.Until}
	b, err := json.Marshal(search)
	if err != nil {
		panic(err)
	}

	return farm.Hash64(b)
}
