package main

import (
	"log"
	"time"
	"context"
	"io/ioutil"
	"google.golang.org/api/iterator"
	"cloud.google.com/go/storage"
	"github.com/fernet/fernet-go"
)

func picnic(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getTokens(location, secret string) map[string]string {

	ctx := context.Background()
	client, _ := storage.NewClient(ctx)
	bkt := client.Bucket(location)

	tokens := make(map[string]string)

	it := bkt.Objects(ctx, &storage.Query{Prefix: ""})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		picnic(err)

		key := attrs.Name
		r, _ := bkt.Object(key).NewReader(ctx)

		tok, err := ioutil.ReadAll(r)
		picnic(err)

		k := fernet.MustDecodeKeys(secret)
		secret := fernet.VerifyAndDecrypt(tok, time.Duration(0), k)

		tokens[key] = string(secret)
	}

	return tokens
}
