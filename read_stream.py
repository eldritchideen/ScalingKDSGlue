import boto3


STREAM_NAME = "TestStream"

kds_client = boto3.client("kinesis")

resp = kds_client.get_shard_iterator(
    StreamName=STREAM_NAME,
    ShardId='shardId-000000000048',
    ShardIteratorType='TRIM_HORIZON'
)

print(resp)

ShardIterator = resp['ShardIterator']

resp = kds_client.get_records(
    ShardIterator=ShardIterator,
    Limit=10
)

print(resp)