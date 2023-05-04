# s3smash.py
#
# By Boni Bruno
#
# License: GNU
#
import threading
import boto3
import botocore
import requests
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

session = boto3.Session()
credentials = session.get_credentials()

region = 'us-east-1'

# set up boto3 S3 client
s3 = boto3.client('s3')

# get input from user
object_size = int(input("Enter object size in bytes: "))
num_objects = int(input("Enter number of objects to upload/download: "))
num_threads = int(input("Enter number of threads to use for upload/download: "))

results = {'upload': [], 'download': [], 'upload_throughput': [], 'download_throughput': []}  # to store results of test

upload_times = []
download_times = []

# define function to upload objects in parallel, don't forget to update Bucket name accordingly.
def upload_object(thread_id):
    for i in range(int(num_objects / num_threads)):
        obj_name = f"test-object-{thread_id}-{i}"
        data = b"0" * object_size
        start_time = datetime.now()
        s3.put_object(Bucket='testbucket--bonibruno', Key=obj_name, Body=data)
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        results['upload'].append(elapsed_time.total_seconds())
        throughput = object_size / elapsed_time.total_seconds()
        results['upload_throughput'].append(throughput)

# define function to download objects in parallel, update Bucket name here as well.
def download_object(thread_id):
    for i in range(int(num_objects / num_threads)):
        obj_name = f"test-object-{thread_id}-{i}"
        start_time = datetime.now()
        s3.get_object(Bucket='testbucket--bonibruno', Key=obj_name)
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        results['download'].append(elapsed_time.total_seconds())
        throughput = object_size / elapsed_time.total_seconds()
        results['download_throughput'].append(throughput)

# define function to calculate throughput in parallel
def calculate_throughput(upload_times, download_times, object_sizes):
    upload_throughputs = []
    download_throughputs = []
    for i in range(len(object_sizes)):
        upload_throughputs.append(object_sizes[i] / upload_times[i])
        download_throughputs.append(object_sizes[i] / download_times[i])
    return upload_throughputs, download_throughputs

# run upload test
upload_threads = []
start_time = datetime.now()
for i in range(num_threads):
    t = threading.Thread(target=upload_object, args=(i,))
    upload_threads.append(t)
    t.start()

for t in upload_threads:
    t.join()

end_time = datetime.now()
elapsed_time = end_time - start_time
print(f"Upload test completed in {elapsed_time.total_seconds():.2f} seconds")

# run download test
download_threads = []
start_time = datetime.now()
for i in range(num_threads):
    t = threading.Thread(target=download_object, args=(i,))
    download_threads.append(t)
    t.start()

for t in download_threads:
    t.join()

end_time = datetime.now()
elapsed_time = end_time - start_time
print(f"Download test completed in {elapsed_time.total_seconds():.2f} seconds")

# print time results
print(f"Upload time results (seconds): {[round(t, 2) for t in results['upload']]}")
print(f"Download time results (seconds): {[round(t, 2) for t in results['download']]}")
print(f"Average upload time (seconds): {round(sum(results['upload']) / len(results['upload']), 2)}")
print(f"Average download time (seconds): {round(sum(results['download']) / len(results['download']), 2)}")
print(f"Minimum upload time (seconds): {round(min(results['upload']), 2)}")
print(f"Minimum download time (seconds): {round(min(results['download']), 2)}")
print(f"Maximum upload time (seconds): {round(max(results['upload']), 2)}")
print(f"Maximum download time (seconds): {round(max(results['download']), 2)}")

# calculate throughput results in MB/s, divide by 1000000
upload_throughputs = [object_size / t for t in results['upload']]
download_throughputs = [object_size / t for t in results['download']]
average_upload_throughput = sum(upload_throughputs) / len(upload_throughputs) / 1000000
average_download_throughput = sum(download_throughputs) / len(download_throughputs) / 1000000
min_upload_throughput = np.amin(upload_throughputs) / 1000000
max_upload_throughput = np.amax(upload_throughputs) / 1000000
min_download_throughput = np.amin(download_throughputs) / 1000000
max_download_throughput = np.amax(download_throughputs) / 1000000

# print results
print(f"Upload throughput results (megabytes/second): {[round(t/1000000, 2) for t in upload_throughputs]}")
print(f"Download throughput results (megabytes/second): {[round(t/1000000, 2) for t in download_throughputs]}")
print(f"Average upload throughput (megabytes/second): {round(average_upload_throughput, 2)}")
print(f"Average download throughput (megabytes/second): {round(average_download_throughput, 2)}")
print(f"Minimum upload throughput (megabytes/second): {round(min_upload_throughput, 2)}")
print(f"Maximum upload throughput (megabytes/second): {round(max_upload_throughput, 2)}")
print(f"Minimum download throughput (megabytes/second): {round(min_download_throughput, 2)}")
print(f"Maximum download throughput (megabytes/second): {round(max_download_throughput, 2)}")

# plot upload and download times
plt.figure()
plt.title(f'S3 Time Results for {num_threads} threads and {object_size} byte objects')
plt.xlabel('Object')
plt.ylabel('Time (s)')
plt.plot(results['upload'])
plt.plot(results['download'])
plt.legend(['Upload','Download'])

# save the time results
plt.savefig("times.png")

# plot upload and download throughputs
plt.figure()
plt.title(f'S3 Throughput Results for {num_threads} threads and {object_size} byte objects')
plt.xlabel('Object')
plt.ylabel('Throughput (MB/s)')
plt.plot(np.array(results['upload_throughput'])/1000000)
plt.plot(np.array(results['download_throughput'])/1000000)
plt.legend(['Upload','Download'])

# save the througput results
plt.savefig("throughputs.png")
