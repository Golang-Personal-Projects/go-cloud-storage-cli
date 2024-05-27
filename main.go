package main

import (
	"bytes"
	"cloud-storage/utils"
	"cloud.google.com/go/storage"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

const (
	projectID  = "xxxxx"
	bucketName = "xxxx"
	location   = "xxxx"
)

type filenames []string

// String is the method to format the flag's value, part of the flag.Value interface.
// The String method's output will be used in diagnostics.
func (f *filenames) String() string {
	return fmt.Sprint(*f)
}

// Set is the method to set the flag value, part of the flag.Value interface.
// Set's argument is a string to be parsed to set the flag.
// It's a comma-separated list, so we split it.
func (f *filenames) Set(value string) error {
	*f = strings.Split(value, ",")
	return nil
}

var filename filenames
var action string

func main() {
	// Tie the command-line flag to the intervalFlag variable and
	// set a usage message.
	flag.Var(&filename, "filename", "filename(s) to be uploaded or deleted")
	flag.StringVar(&action, "action", "upload", "download, upload , delete  or deleteAll ")

	flag.Parse()

	if flag.NFlag() < 1 {
		flag.Usage()
		os.Exit(1)
	} else {
		fmt.Printf("Here are the values for filenames: %v\n", filename)
	}
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	buf := &bytes.Buffer{}

	sd := utils.StorageData{
		Client:     client,
		BucketName: bucketName,
		Ctx:        ctx,
		Bucket:     client.Bucket(bucketName),
		Location:   location,
		ProjectID:  projectID,
		Wc:         buf,
	}
	fmt.Println(sd)

	switch action {
	case "upload":
		for _, filename := range filename {
			err = sd.UploadObject(filename)
			if err != nil {
				log.Fatalln(err)
			}
		}
	case "download":
		for _, filename := range filename {
			err = sd.DownloadObject(filename)
			if err != nil {
				log.Fatalln(err)
			}
		}
	case "delete":
		for _, filename := range filename {
			err = sd.DeleteObject(filename)
			if err != nil {
				log.Fatalln(err)
			}
		}
	case "deleteAll":
		err := sd.DeleteAllObjects()
		if err != nil {
			log.Fatalln(err)
		}
	default:
		flag.Usage()
		fmt.Println("provide an action to be done\n" +
			"\t upload - to upload a file\n" +
			"\t download - to download a file\n" +
			"\t delete - to delete a file\n" +
			"\t deleteAll - to delete all objects older than 30 days",
		)
	}
}
