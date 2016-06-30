import urllib2
import io
import gzip
from minio import Minio

def getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path):
    minioClient = Minio(minioHost+":"+minioPort, access_key=accessKey, secret_key=secretKey, secure=False)
    data = minioClient.get_object(bucket, path)
    compressed = io.BytesIO(data.read())
    decompressed = gzip.GzipFile(fileobj=compressed)
    return decompressed
        
def getMinioPaths(minioHost, minioPort, accessKey, secretKey, bucket, path):
    minioClient = Minio(minioHost+":"+minioPort, access_key=accessKey, secret_key=secretKey, secure=False)
    objects = minioClient.list_objects(bucket, prefix=path, recursive=True)
    
    paths = []
    
    for obj in objects:
        paths.append(obj.object_name.encode('utf-8'))
    return paths

def getFromUrl(url):
    res = urllib2.urlopen(url)
    compressed = io.BytesIO(res.read())
    decompressed = gzip.GzipFile(fileobj=compressed)
    lines = decompressed.readlines()
    return lines