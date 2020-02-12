import os
import boto3

S3_BUCKET_NAME = "jex-bucket"
OLD_FOLDER_KEY = "000000000000000101"
NEW_FOLDER_KEY = "111000000000000101"

s3 = boto3.resource('s3')

def renameMainFolder(bucketName, oldFolderKey, newFolderKey):
    deleteOldFolderKey = False    
    bucket = s3.Bucket(bucketName)
    for object in bucket.objects.filter(Prefix=oldFolderKey):
        srcKey = object.key
        print(f'srcKey -> {srcKey}')
        if not srcKey.endswith('/'):
            fileName = srcKey.split('/')[-1]
            destFileKey = newFolderKey + '/' + fileName
            copySource = bucketName + '/' + srcKey         
            s3.Object(bucketName, destFileKey).copy_from(CopySource=copySource)
            s3.Object(bucketName, srcKey).delete()
            deleteOldFolderKey = True
        else:
            oldFolderKeyToDelete = srcKey

    if (oldFolderKeyToDelete != '' and deleteOldFolderKey):
        s3.Object(bucketName, oldFolderKeyToDelete).delete()

def renameStoreFileInFolder(bucketName, folder, fileName, newFileName):
    bucket = s3.Bucket(bucketName)
    for object in bucket.objects.filter(Prefix=folder):
        srcKey = object.key
        print(f'srcKey -> {srcKey}')
        if not srcKey.endswith('/'):
            fileName = srcKey.split('/')[-1]
            fileExt =fileName.split('.')[-1]
            print(f'fileName: {fileName} / fileExt: {fileExt}')
            destFileKey = folder + '/' + newFileName + '.' + fileExt
            print(f'destFileKey : {destFileKey}')
            copySource = bucketName + '/' + srcKey         
            s3.Object(bucketName, destFileKey).copy_from(CopySource=copySource)
            s3.Object(bucketName, srcKey).delete()

def main():
    renameMainFolder(S3_BUCKET_NAME,OLD_FOLDER_KEY, NEW_FOLDER_KEY)
    renameStoreFileInFolder(S3_BUCKET_NAME, 'store', OLD_FOLDER_KEY, NEW_FOLDER_KEY)

if __name__ == "__main__":
    main()
