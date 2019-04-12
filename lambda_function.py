#!/usr/bin/env python

import json
import re
import boto3
import psycopg2
import requests

_BUCKET='<your_bucket_name>'
#_PREFIX='symbols'
_PREFIX='symbols-test'

_DB_HOST = "<your_db_host>"
_DB_PORT = 5439
_DB_NAME = "<your_db_name>"
_DB_USER = "<your_db_user>"
_DB_PASS = "<your_db_password>"
_DB_TABLE = "stock"
_DB_TIMEOUT = 10

#event = {
#  "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
#  "detail-type": "Scheduled Event",
#  "source": "aws.events",
#  "account": "741251161495",
#  "time": "1970-01-01T00:00:00Z",
#  "region": "cn-northwest-1",
#  "resources": [
#    "arn:aws-cn:events:cn-northwest-1:123456789012:rule/ExampleRule"
#  ],
#  "detail": {}
#}

client = boto3.resource('s3', region_name = 'cn-northwest-1')
s3 = client.Bucket(_BUCKET)

def _make_conn():
    conn = None
    try:
        conn = psycopg2.connect(
            "dbname='%s' user='%s' host='%s' port='%s' password='%s' connect_timeout='%s'" \
                % (_DB_NAME, _DB_USER, _DB_HOST, _DB_PORT, _DB_PASS, _DB_TIMEOUT)
        )
    except:
        print "I am unable to connect to the database"

    return conn

def _execute_cmd(conn, query):
    print "Now executing: %s" % (query)
    cursor = conn.cursor()
    cursor.execute(query)
    #conn.commit()
    return True

def _copy_to_redshift(table, bucket_key_pair):
    """
    Copy a list of files to Redshift
    params: table, table name
    params: bucket_key_pair, a list of (bucket, key) to copy
    return: True/False
    """
    query_cmd = ""
    for _ in bucket_key_pair:
        bucket, key = _
        # s3://<bucket_name>/symbols/DOX.csv
        file = 's3://' + bucket + '/' + key
        
        query_cmd = query_cmd + """
copy %s from '%s'
credentials '<your_iam_role_to_write_to_Redshift>'
delimiter ',' region 'cn-north-1'
dateformat 'auto';
""" % (table, file) + "\n"

    conn = _make_conn()
    _execute_cmd(conn, query_cmd)

    try:
        conn.commit()
    except:
        print('commit failed')
        return False
    else:
        print('commit succ')
        # Remove objects after committed
        for _ in bucket_key_pair:
            bucket, key = _
            s3.Object(key).delete()
            print('deleted %s' % key)

        return True
    
def _get_object_list(prefix):
    """
    Get a list of object to copy to Redshift
    params:
    return: object_list
    """
    #files_to_copy = []
    bucket_key_pair = []
    for object in s3.objects.filter(Prefix=prefix):
        #print(object.key)
        if re.match('.*\.csv$', object.key):
            #print('s3://' + object.bucket_name + '/' + object.key)
            #files_to_copy.append('s3://' + object.bucket_name + '/' + object.key)
            bucket_key_pair.append((object.bucket_name, object.key))
        else:
            pass
    return bucket_key_pair

def lambda_handler(event, context):
    """
    List all objects in staging folder
    Copy to redshift
    Remove copied object
    params: None
    return True/False
    """

    bucket_key_pair = _get_object_list(_PREFIX)
    print(bucket_key_pair[:3])

    if not bucket_key_pair:
        print('No objects found!')
        return False
    else:
        _copy_to_redshift(_DB_TABLE, bucket_key_pair)
        return True


if __name__ == '__main__':
    lambda_handler(None, None)
