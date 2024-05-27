package utils

import (
	"cloud.google.com/go/storage"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	"io"
	"os"
	"time"
)

// UploadObject function can be used to send files to GCP Cloud Storage
func (sd *StorageData) UploadObject(fileName string) error {
	fmt.Printf("Sending file %v to GCP bucket %v!!!! \n", fileName, sd.BucketName)

	wc := sd.Bucket.Object(fileName).NewWriter(sd.Ctx)

	file, err := os.Open(fileName)
	if err != nil {
		return sd.errorf("UploadObject: Error opening file %v: %v", fileName, err)
	}

	defer file.Close()

	_, err = io.Copy(wc, file)

	if err != nil {
		return sd.errorf("UploadObject: Error copying %q to bucket %q: %v", sd.BucketName, fileName, err)
	}

	// Close, just like writing a file. File appears in GCS after
	if err := wc.Close(); err != nil {
		return sd.errorf("UploadObject: unable to close bucket %q, file %q: %v", sd.BucketName, fileName, err)

	}

	fmt.Printf("Completed Sending file %v to GCP bucket %v \n", fileName, sd.BucketName)
	return nil
}

// ReadObject function
func (sd *StorageData) ReadObject(objectName string) error {
	fmt.Printf("Reading file %v from GCP bucket %v!!!! \n", objectName, sd.BucketName)

	rd, err := sd.Bucket.Object(objectName).NewReader(sd.Ctx)
	if err != nil {
		return sd.errorf("readFile: unable to open file from bucket %q, file %q: %v", sd.BucketName, objectName, err)

	}

	defer rd.Close()

	data, err := io.ReadAll(rd)
	if err != nil {
		return sd.errorf("readFile: unable to read file from bucket %q, file %q: %v", sd.BucketName, objectName, err)

	}

	fmt.Printf("File Contents below %v\n")
	fmt.Println(string(data))
	return nil
}

// DownloadObject function
func (sd *StorageData) DownloadObject(objectName string) error {
	fmt.Printf("Downloading file %v from GCP bucket %v!!!! \n", objectName, sd.BucketName)

	file, err := os.Create(objectName)
	if err != nil {
		return sd.errorf("unable to create a new destination file %q : %v", objectName, err)

	}
	defer file.Close()

	rd, err := sd.Bucket.Object(objectName).NewReader(sd.Ctx)

	defer rd.Close()

	_, err = io.Copy(file, rd)
	if err != nil {
		return sd.errorf("unable to copy obect data from bucket %q, to destination file %q: %v", sd.BucketName, objectName, err)

	}

	fmt.Fprintf(os.Stdout, "Blob object download completed\n")
	return nil
}

// DeleteObject Function
func (sd *StorageData) DeleteObject(objectName string) error {
	fmt.Printf("Deleting Blob Object %q from GCP bucket %v!!!! \n", objectName, sd.BucketName)

	err := sd.Bucket.Object(objectName).Delete(sd.Ctx)
	if err != nil {
		return sd.errorf("deleteFile: unable to delete object %q from %q bucket: %v", objectName, sd.BucketName, err)
	}

	fmt.Fprintf(os.Stdout, "Blob %v deleted.\n", objectName)
	return nil
}

// DeleteAllObjects Function
func (sd *StorageData) DeleteAllObjects() error {
	fmt.Printf("Deleting All Blob Object older than 30 days from GCP bucket !!!! \n")

	query := &storage.Query{
		Versions: true,
	}
	objectIterator := sd.Bucket.Objects(sd.Ctx, query)
	for {
		objectAttrs, err := objectIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return sd.errorf("DeleteAllObjects: unable to iterate objects in bucket %q: %v", sd.BucketName, err)
		}

		// Determine blob object length of days
		daysCreated := int(objectAttrs.Created.Sub(time.Now()).Hours()) / 24
		fmt.Println(daysCreated)
		if daysCreated > 30 || daysCreated == 30 {
			fmt.Printf("Deleting object %v from bucket", objectAttrs.Name)
			err = sd.Bucket.Object(objectAttrs.Name).Delete(sd.Ctx)
			if err != nil {
				err = sd.errorf("DeleteAllObjects: unable to delete object %q: %v", objectAttrs.Name, err)
			}
		}
	}

	return nil

}
