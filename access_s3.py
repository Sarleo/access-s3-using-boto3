

'''
from __future__ import absolute_import, print_function, unicode_literals
from io import BytesIO
from gzip import GzipFile
import boto3
import csv
s3 = boto3.client('s3')
bucket = 's3://analyst-adhoc/Saransh/check/'
with open("Saransh_FosterGrant_dump_2018-08-222_retain_000.gz") as fi:
    text_body = fi.read().decode("utf-8")
gz_body = BytesIO()
gz = GzipFile(None, 'wb', 9, gz_body)
gz.write(text_body.encode('utf-8'))  # convert unicode strings to bytes!
gz.close()

s3.put_object(
    Bucket=bucket,
    Key='gztest.txt',  # Note: NO .gz extension!
    ContentType='text/plain',  # the original type
    ContentEncoding='gzip',  # MUST have or browsers will error
    Body=gz_body.getvalue()
)

retr = s3.get_object(Bucket=bucket, Key='gztest.txt')

bytestream = BytesIO(retr['Body'].read())
got_text = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
assert got_text == text_body
'''

import boto3
import io
from io import StringIO
from gzip import GzipFile
import gzip
import os

os.environ["AWS_ACCESS_KEY_ID"] = "xxxx"
os.environ["AWS_SECRET_ACCESS_KEY"] = "yyyy"

s3 = boto3.client('s3')
def s3_to_pandas(client, bucket, key, header=None, delimiter='|'):
  obj = client.get_object(Bucket=bucket, Key='')
  gz = gzip.GzipFile(fileobj=obj['Body'])
  return pd.read_csv(gz, header=header, dtype=str, sep=delimiter)

bucket="zzzz"
filename = "aaaa/aaa/ddddd.gz"
everbank_header = ['date']
saransh_dates= s3_to_pandas(s3, bucket, filename, header=None)

vdna = s3_to_pandas(s3,"s3://zzzz/aaaa/aaaaa/",)
