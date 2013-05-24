import boto
from gzip import GzipFile
import StringIO

def arc_file(s3,  info, bucket="aws-publicdatasets"):
    bucket = s3.lookup(bucket)
    keyname = "/common-crawl/parse-output/segment/{arcSourceSegmentId}/{arcFileDate}_{arcFilePartition}.arc.gz".format(**info)
    key = bucket.lookup(keyname)

    start = info['arcFileOffset']
    end = start + info['compressedSize'] - 1

    headers={'Range' : 'bytes={}-{}'.format(start, end)}

    chunk = StringIO.StringIO(key.get_contents_as_string(headers=headers))

    return GzipFile(fileobj=chunk).read()

if __name__ == "__main__":
    conn  = boto.connect_s3(anon=True)
    data = {'compressedSize': 8027, 'arcSourceSegmentId': 1346876860782L, 'arcFilePartition': 2301, 'arcFileDate': 1346909213517L, 'arcFileOffset': 13395666}

    print arc_file(conn, data)
