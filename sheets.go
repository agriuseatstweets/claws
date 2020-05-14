package main

import (
	"log"
	"context"
	"strings"
	"os"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
)

func flattenValues(res *sheets.ValueRange) []string {
	var vals []string
	for _, i := range res.Values {
		for _, j := range i {
			s, ok := j.(string)
			if ok {
				s = strings.TrimSpace(s)
				if s != "" {
					vals = append(vals, s)
				}
			}
		}
	}
	return vals
}

func getValues(sheet, rng string) []string {
	client, err := google.DefaultClient(context.Background(),
		"https://www.googleapis.com/auth/spreadsheets.readonly")

	if err != nil {
		log.Fatalf("Unable to create google client: %v", err)
	}

	srv, err := sheets.New(client)

	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	res, err := srv.Spreadsheets.Values.Get(sheet, rng).Do()

	if err != nil {
		log.Fatalf("Unable to get Sheets values.\nSheet: %v\nRange: %v\nError:\n %v", sheet, rng, err)
	}

	return flattenValues(res)
}

func getAllValues(sheet, rng string) []string {
	a := strings.Split(rng, ",")
	var vals []string
	for _, r := range a {
		r = strings.TrimSpace(r)
		vals = append(vals, getValues(sheet, r)...)
	}
	return vals
}

func getLocations () []string {
	sheet := os.Getenv("CLAWS_SHEET_ID")
	rng := os.Getenv("CLAWS_SHEET_LOCATIONS")
	return getAllValues(sheet, rng)
}
