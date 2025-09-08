from dataflux_core import fast_list, download
import argparse
import os
import json
import time
from google.cloud import storage

def write_dict_to_json(filename, data):
    """
    Args:
        data (dict): The dictionary to be converted to JSON.
        filename (str): The name of the JSON file to be created.
    """
    print(f"writing data {data} to file {filename}")
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
def calculate(data):
    """
    Args:
        data: A list of tuples where the first element is a string and the second element is an integer.
    Returns:
        The list len, and median of the integer elements in the data.
    """
    object_sizes = [item[1] for item in data]
    sorted_data = sorted(object_sizes)
    num_objects = len(sorted_data)
    total_size = sum(sorted_data)
    if num_objects % 2 == 0:
        mid1 = num_objects // 2 - 1
        mid2 = mid1 + 1
        median_size = (sorted_data[mid1] + sorted_data[mid2]) / 2
    else:
        mid = num_objects // 2
        median_size = sorted_data[mid]
    return int(num_objects), int(median_size), int(total_size)
def dflist(project, bucket, workers):
    list_start_time = time.time()
    print(f"Listing operation started at {list_start_time}")
    list_result = fast_list.ListingController(int(workers), project, bucket).run()
    list_end_time = time.time()
    print(f"{len(list_result)} objects listed in {list_end_time - list_start_time} seconds")
    return calculate(list_result)
def write_dict_to_json(filename, data):
    """
    Args:
        data (dict): The dictionary to be converted to JSON.
        filename (str): The name of the JSON file to be created.
    """
    print(f"writing data {data} to file {filename}")
    with open(filename, 'w') as f:
        json.dump(data, f)
def main():
    parser = argparse.ArgumentParser(description="Dataflux lister to scan GCS Buckets")
    parser.add_argument("-p", "--project", help="GCP project of the GCS bucket")
    parser.add_argument("-b", "--bucket", help="GCS bucket name")
    parser.add_argument("-w", "--workers", help="Number of workers")
    parser.add_argument("-o", "--outputfile", help="Output file name which captures the bucket info")
    args = parser.parse_args()
    bucket_file_name = args.outputfile
    project = args.project
    bucket = args.bucket
    workers = args.workers
    # print ("input:", project, bucket, args.workers)
    num_objects, median_object_size_bytes, total_size_bytes = dflist(project, bucket, workers)
    hns_enabled = False # Default
    try:
        storage_client = storage.Client(project=project)
        bucket = storage_client.get_bucket(bucket)
        if bucket.hierarchical_namespace and bucket.hierarchical_namespace.enabled:
             hns_enabled = True
        print(f"Bucket {bucket} HNS Enabled: {hns_enabled}")
    except Exception as e:
        print(f"Error getting HNS status for bucket {bucket}: {e}")

    write_dict_to_json(bucket_file_name, {
        "num_objects" : num_objects,
        "median_object_size_bytes" : median_object_size_bytes,
        "total_size_bytes": total_size_bytes,
        "hns_enabled": hns_enabled,
    })
if __name__ == "__main__":
    main()