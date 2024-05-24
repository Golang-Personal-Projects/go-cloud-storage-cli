package main

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"os"
	"time"
)

type storageData struct {
	client     *storage.Client
	bucket     *storage.BucketHandle
	bucketName string
	location   string
	projectID  string

	ctx    context.Context
	wc     io.ReadWriter
	failed bool
}

func (sd *storageData) errorf(format string, args ...interface{}) {
	sd.failed = true
	fmt.Println(sd.wc, fmt.Sprintf(format, args...))

}

func main() {

	var (
		projectID  = "718591994558"
		bucketName = "miluabucket"
		location   = "us-east1"
	)

	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	buf := &bytes.Buffer{}

	sd := storageData{
		client:     client,
		bucketName: bucketName,
		ctx:        ctx,
		bucket:     client.Bucket(bucketName),
		location:   location,
		projectID:  projectID,
		wc:         buf,
	}

	// Create New Bucket
	//bucketAttributes := &storage.BucketAttrs{
	//	Name:     bucketName,
	//	Location: location,
	//}
	//
	//err = createBucket(ctx, client, bucketsIterator, projectID, bucketAttributes)
	//if err != nil {
	//	log.Fatalf("unable to create bucket: %s", err)
	//}
	//
	//time.Sleep(time.Duration(5) * time.Minute)

	// deleteBucket
	//if err := deleteBucket(ctx, client, bucketsIterator, bucketName); err != nil {
	//	log.Fatalf("unable to delete bucket: %s failed %s", bucketName, err)
	//}

	// update Bucket
	//newBucketAttributes := storage.BucketAttrsToUpdate{
	//	VersioningEnabled:      true,
	//	StorageClass:           "STANDARD",
	//	PublicAccessPrevention: storage.PublicAccessPreventionEnforced,
	//}
	//
	//if err := updateBucket(ctx, client, bucketsIterator, projectID, bucketName, newBucketAttributes); err != nil {
	//	log.Fatalf("unable to update bucket: %s failed %s", bucketName, err)
	//}

	// put object in the bucket
	//sd.putFile("data.csv")

	// read object in the bucket
	sd.readFile("data.csv")

	// download object
	sd.downloadFile("data.csv", "data.csv")

	// List bucket Contents
	sd.listBucket()

	sd.deleteFile("data.csv")

}

// listBucket Function
func (sd *storageData) listBucketObjects() {
	fmt.Fprintf(os.Stdout, "Listing objects in %s bucket\n", sd.bucketName)

	query := &storage.Query{}
	it := sd.bucket.Objects(sd.ctx, query)
	for {
		obj, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			sd.errorf("listBucket: failed to list %q contents: %v", sd.bucketName, err)
			os.Exit(1)
		}
		sd.dumpObjStats(obj)
	}
}

// dumpObjStats Function
func (sd *storageData) dumpObjStats(obj *storage.ObjectAttrs) {
	fmt.Printf("(filename: /%v/%v, ", obj.Bucket, obj.Name)
	fmt.Printf("ContentType: %q, ", obj.ContentType)
	fmt.Printf("ACL: %#v, ", obj.ACL)
	fmt.Printf("Owner: %v, ", obj.Owner)
	fmt.Printf("ContentEncoding: %q, ", obj.ContentEncoding)
	fmt.Printf("Size: %v, ", obj.Size)
	fmt.Printf("MD5: %q, ", obj.MD5)
	fmt.Printf("CRC32C: %q, ", obj.CRC32C)
	fmt.Printf("Metadata: %#v, ", obj.Metadata)
	fmt.Printf("MediaLink: %q, ", obj.MediaLink)
	fmt.Printf("StorageClass: %q, ", obj.StorageClass)
	if !obj.Deleted.IsZero() {
		fmt.Printf("Deleted: %v, ", obj.Deleted)
	}
	fmt.Printf("Updated: %v)\n", obj.Updated)
}

// dumpBucketStats Function
func (sd *storageData) dumpBucketStats(bucketAttrs *storage.BucketAttrs) {
	fmt.Printf("(BucketName: /%v, ", bucketAttrs.Name)
	fmt.Printf("VersioningEnabled: %v, ", bucketAttrs.VersioningEnabled)
	fmt.Printf("Loction: %#v, ", bucketAttrs.Location)
	fmt.Printf("Bucket Creation Date: %v, ", bucketAttrs.Created)
	fmt.Printf("PublicAccess: %q, ", bucketAttrs.PublicAccessPrevention)
	fmt.Printf("StorageClass: %v, ", bucketAttrs.StorageClass)
	fmt.Printf("BucketRetentionPolicy: %q, ", bucketAttrs.RetentionPolicy)
}

// listBucket
func (sd *storageData) listBucket() {
	fmt.Fprintf(os.Stdout, "Listing buckets\n")

	it := sd.client.Buckets(sd.ctx, sd.projectID)
	for {
		bucketAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			sd.errorf("", err)
		}
		sd.dumpBucketStats(bucketAttrs)
	}
	for {
		obj, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			sd.errorf("listBucket: failed to list %q contents: %v", sd.bucketName, err)
			os.Exit(1)
		}
		sd.dumpBucketStats(obj)
	}
}

// createBucket Function
func (sd *storageData) createBucket(bucketAttributes *storage.BucketAttrs) error {
	//bucketsIterator := client.Buckets(ctx, projectID)

	bucketHandle := sd.client.Bucket(sd.bucketName)

	err := bucketHandle.Create(sd.ctx, sd.projectID, bucketAttributes)
	if err != nil {
		return err
	} else {
		fmt.Println("created bucket:", bucketAttributes.Name)
	}

	return nil
}

// deleteBucket Function
func (sd *storageData) deleteBucket() error {
	//bucketsIterator := client.Buckets(ctx, projectID)
	//for {
	//	bucketAttrs, err := bucketsIterator.Next()
	//	if errors.Is(err, iterator.Done) {
	//		break
	//	}
	//	if err != nil {
	//		return err
	//	}
	//	if bucketAttrs.Name != bucketName {
	//		return fmt.Errorf("%s bucket does not exists\n", bucketName)
	//	}
	//}

	bucketHandle := sd.client.Bucket(sd.bucketName)
	err := bucketHandle.Delete(sd.ctx)
	if err != nil {
		return err
	} else {
		fmt.Println("deleted bucket:", sd.bucketName)
	}

	return nil
}

// updateBucket Function
func (sd *storageData) updateBucket(newBucketAttributes storage.BucketAttrsToUpdate) error {
	//bucketsIterator := client.Buckets(ctx, projectID)
	//for {
	//	bucketAttrs, err := bucketsIterator.Next()
	//	if errors.Is(err, iterator.Done) {
	//		break
	//	}
	//	if err != nil {
	//		return err
	//	}
	//	if bucketAttrs.Name != bucketName {
	//		return fmt.Errorf("%s bucket does not exists\n", bucketName)
	//	}
	//}

	bucketHandle := sd.client.Bucket(sd.bucketName)
	newBucketAttr, err := bucketHandle.Update(sd.ctx, newBucketAttributes)
	if err != nil {
		return err
	} else {
		fmt.Printf("updated bucket  %v:", newBucketAttr.Name)
	}

	return nil
}

// putFile function can be used to send files to GCP Cloud Storage
func (sd *storageData) putFile(fileName string) {
	fmt.Printf("Sending file %v to GCP bucket %v!!!! \n", fileName, sd.bucketName)

	wc := sd.bucket.Object(fileName).NewWriter(sd.ctx)

	file, _ := os.Open(fileName)

	defer file.Close()

	_, err := io.Copy(wc, file)

	if err != nil {
		sd.errorf("unable to copy file to bucket %q, file %q: %v", sd.bucketName, fileName, err)
		os.Exit(1)
	}

	// Close, just like writing a file. File appears in GCS after
	if err := wc.Close(); err != nil {
		sd.errorf("createFile: unable to close bucket %q, file %q: %v", sd.bucketName, fileName, err)
		os.Exit(1)
	}

	fmt.Printf("Completed Sending file %v to GCP bucket %v \n", fileName, sd.bucketName)

}

// readFile function
func (sd *storageData) readFile(objectName string) {
	fmt.Printf("Reading file %v from GCP bucket %v!!!! \n", objectName, sd.bucketName)

	rd, err := sd.bucket.Object(objectName).NewReader(sd.ctx)
	if err != nil {
		sd.errorf("readFile: unable to open file from bucket %q, file %q: %v", sd.bucketName, objectName, err)
		os.Exit(1)
	}

	defer rd.Close()

	data, err := io.ReadAll(rd)
	if err != nil {
		sd.errorf("readFile: unable to read file from bucket %q, file %q: %v", sd.bucketName, objectName, err)
		os.Exit(1)
	}

	fmt.Printf("File Contents below %v\n")
	fmt.Println(string(data))

}

// downloadFile function
func (sd *storageData) downloadFile(destFileName, objectName string) {
	fmt.Printf("Downloading file %v from GCP bucket %v!!!! \n", objectName, sd.bucketName)

	file, err := os.Create(destFileName)
	if err != nil {
		sd.errorf("unable to create a new destination file %q : %v", destFileName, err)
		os.Exit(1)
	}
	defer file.Close()

	rd, err := sd.bucket.Object(objectName).NewReader(sd.ctx)

	defer rd.Close()

	_, err = io.Copy(file, rd)
	if err != nil {
		sd.errorf("unable to copy obect data from bucket %q, to destination file %q: %v", sd.bucketName, destFileName, err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Blob %v downloaded to local file %v\n", objectName, destFileName)

}

// deleteFile Function
func (sd *storageData) deleteFile(objectName string) {
	fmt.Printf("Deleting Blob Object %q from GCP bucket %v!!!! \n", objectName, sd.bucketName)

	err := sd.bucket.Object(objectName).Delete(sd.ctx)
	if err != nil {
		sd.errorf("deleteFile: unable to delete object %q from %q bucket: %v", objectName, sd.bucketName, err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Blob %v deleted.\n", objectName)
}
