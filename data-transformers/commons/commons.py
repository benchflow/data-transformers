import urllib2
import io
import gzip

def getFromUrl(url):
    res = urllib2.urlopen(url)
    compressed = io.BytesIO(res.read())
    decompressed = gzip.GzipFile(fileobj=compressed)
    lines = decompressed.readlines()
    return lines