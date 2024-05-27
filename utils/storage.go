package utils

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	"io"
	"os"
)

type StorageData struct {
	Client     *storage.Client
	Bucket     *storage.BucketHandle
	BucketName string
	Location   string
	ProjectID  string
	Ctx        context.Context
	Wc         io.ReadWriter
	failed     bool
}

func (sd *StorageData) errorf(format string, args ...interface{}) error {
	sd.failed = true
	//.Println(sd.Wc, fmt.Sprintf(format, args...))
	return fmt.Errorf(format, args...)
}

// ListBucketObjects  Function
func (sd *StorageData) ListBucketObjects() ([]string, error) {
	//fmt.Fprintf(os.Stdout, "Listing objects in %s bucket\n", sd.BucketName)

	var objectSlice []string
	query := &storage.Query{}
	it := sd.Bucket.Objects(sd.Ctx, query)
	for {
		obj, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, sd.errorf("listBucketObjects: failed to list %q contents: %v", sd.BucketName, err)

		}
		//		sd.dumpObjStats(obj)

		objectSlice = append(objectSlice, obj.Name)
	}
	return objectSlice, nil
}

// dumpObjStats Function
func (sd *StorageData) dumpObjStats(obj *storage.ObjectAttrs) {
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
func (sd *StorageData) dumpBucketStats(bucketAttrs *storage.BucketAttrs) {
	fmt.Printf("(BucketName: %v, ", bucketAttrs.Name)
	fmt.Printf("VersioningEnabled: %v, ", bucketAttrs.VersioningEnabled)
	fmt.Printf("Loction: %#v, ", bucketAttrs.Location)
	fmt.Printf("Bucket Creation Date: %v, ", bucketAttrs.Created)
	fmt.Printf("PublicAccess: %q, ", bucketAttrs.PublicAccessPrevention)
	fmt.Printf("StorageClass: %v, ", bucketAttrs.StorageClass)
	fmt.Printf("BucketRetentionPolicy: %q\n", bucketAttrs.RetentionPolicy)
}

// ListBucket Function
func (sd *StorageData) ListBucket() error {
	fmt.Fprintf(os.Stdout, "Listing buckets\n")

	//	var buckets []string
	it := sd.Client.Buckets(sd.Ctx, sd.ProjectID)
	for {
		bucketAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return sd.errorf("listBuckets: failed to list buckets in project id %v: %v", sd.ProjectID, err)
		}
		sd.dumpBucketStats(bucketAttrs)
		//buckets = append(buckets, bucketAttrs.Name)
		//fmt.Printf("Buckets: %v\n", buckets)
	}
	return nil
}

// CreateBucket Function
func (sd *StorageData) CreateBucket(bucketAttributes *storage.BucketAttrs) error {
	fmt.Fprintf(os.Stdout, "Creating a new bucket!!!\n")

	// check if the bucket exists or not
	bucketsIterator := sd.Client.Buckets(sd.Ctx, sd.ProjectID)
	for {
		bucketAttrs, err := bucketsIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if bucketAttrs.Name == sd.BucketName {
			return sd.errorf("createBucket: %v bucket already exists in this %v project", sd.BucketName, sd.ProjectID)
		}
		if err != nil {
			return sd.errorf("createBucket: %v bucket failed: %v", sd.BucketName, err)
		}
	}

	bucketHandle := sd.Client.Bucket(sd.BucketName)

	err := bucketHandle.Create(sd.Ctx, sd.ProjectID, bucketAttributes)
	if err != nil {
		return sd.errorf("createBucket: failed to create %q bucket: %v", sd.BucketName, err)
	} else {
		fmt.Println("created bucket:", bucketAttributes.Name)
	}
	return nil
}

// DeleteBucket Function
func (sd *StorageData) DeleteBucket() error {
	fmt.Fprintf(os.Stdout, "Deleting %q bucket\n", sd.BucketName)

	fmt.Printf("Checking if the bucket is Empty!!!\n")
	objectSlice, err := sd.ListBucketObjects()
	if err != nil {
		return err
	}
	if len(objectSlice) > 0 {

		return sd.errorf("The bucket is not empty and cannot be deleted")
	}

	bucketsIterator := sd.Client.Buckets(sd.Ctx, sd.ProjectID)
	for {
		bucketAttrs, err := bucketsIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}
		if bucketAttrs.Name != sd.BucketName {
			return sd.errorf("%s bucket does not exist in project %v\n", sd.BucketName, sd.ProjectID)
		}
	}

	bucketHandle := sd.Client.Bucket(sd.BucketName)
	err = bucketHandle.Delete(sd.Ctx)
	if err != nil {
		return sd.errorf("deleteBucket: failed to delete %q bucket: %v", sd.BucketName, err)
	} else {
		fmt.Println("deleted bucket:", sd.BucketName)
	}

	return nil
}

// UpdateBucket Function
func (sd *StorageData) UpdateBucket(newBucketAttributes storage.BucketAttrsToUpdate) error {
	fmt.Fprintf(os.Stdout, "Updating %q bucket\n", sd.BucketName)

	bucketsIterator := sd.Client.Buckets(sd.Ctx, sd.ProjectID)
	for {
		bucketAttrs, err := bucketsIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}
		if bucketAttrs.Name != sd.BucketName {
			return fmt.Errorf("%s bucket does not exists\n", sd.BucketName)
		}
	}

	bucketHandle := sd.Client.Bucket(sd.BucketName)
	newBucketAttr, err := bucketHandle.Update(sd.Ctx, newBucketAttributes)
	if err != nil {
		return sd.errorf("updateBucket: failed to update %q bucket: %v", sd.BucketName, err)
	} else {
		fmt.Printf("updated bucket: %v\n", sd.BucketName)
		fmt.Printf("Updated Attributes for the bucket %q:", sd.BucketName)
		sd.dumpBucketStats(newBucketAttr)
	}

	return nil
}
