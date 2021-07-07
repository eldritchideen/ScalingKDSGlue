import boto3
import time

glue = boto3.client("glue")
kds = boto3.client("kinesis")


# Find running glue jobs and stop them. 
# This code assumes that there is only one running task per job definition. 
def stop_running_jobs():
    # Find jobs that need to scale up/down, these jobs are tagged 'Scale' : 1
    res = glue.list_jobs(Tags={"Scale": "1"})
    jobs = res["JobNames"]

    # Find the running task
    for job in jobs:
        running_job_ids = [
            run["Id"] for run in glue.get_job_runs(JobName=job)["JobRuns"] if run["JobRunState"] == "RUNNING"
        ]
        assert len(running_job_ids) == 1
        print(f"Running job ID: {running_job_ids[0]}")

        # Stop the running task
        res = glue.batch_stop_job_run(JobName=job, JobRunIds=running_job_ids)
        assert len(res["Errors"]) == 0

        # Wait for job to stop
        print(f"Stopping job {running_job_ids[0]}")
        status = ''
        while status != "STOPPED":
            status = glue.get_job_run(JobName=job, RunId=running_job_ids[0])["JobRun"]["JobRunState"]
            print(status)
            time.sleep(1.5)


# Start jobs back up
def start_jobs(num_workers):
    res = glue.list_jobs(Tags={"Scale": "1"})
    jobs = res["JobNames"]

    for job in jobs:
        res = glue.start_job_run(JobName=job, NumberOfWorkers=num_workers, WorkerType='G.1X')
        print(f'Job Run Id {res["JobRunId"]}')


# Updates Shard Count in Stream and Waits for it become Active
# Note the restrictions on how the shard count can change 
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.update_shard_count
# For example to go from 1 shard to 8, this function would need to be called three times
# udpate_shards('TestStream', 2)
# udpate_shards('TestStream', 4)
# udpate_shards('TestStream', 8)
def update_shards(stream_name, target_shard_count):
    res = kds.update_shard_count(StreamName=stream_name, TargetShardCount=target_shard_count, ScalingType='UNIFORM_SCALING')
    assert(res['TargetShardCount'] == target_shard_count)
    
    status = ''
    while status != 'ACTIVE':
        time.sleep(1.5)
        status = kds.describe_stream(StreamName=stream_name)['StreamDescription']['StreamStatus']
        print(f'Stream {stream_name} is {status}')


# update_shards('TestStream', 2)

# start_jobs()

stop_running_jobs() 