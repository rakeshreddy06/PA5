import pymongo
import time
import random
import string
from statistics import mean
import sys

def generate_random_key_value():
    key = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    value = ''.join(random.choices(string.ascii_letters + string.digits, k=90))
    return key, value

def measure_operation_time(client, operation, key, value=None):
    collection = client.test_db.test_collection
    start_time = time.time()

    if operation == 'insert':
        collection.insert_one({key: value})
    elif operation == 'lookup':
        collection.find_one({key: value})
    elif operation == 'remove':
        collection.delete_one({key: value})

    end_time = time.time()
    return (end_time - start_time) * 1000  # Convert to milliseconds

def perform_test(mongo_uri, num_operations=1000):
    client = pymongo.MongoClient(mongo_uri)
    latencies = {'insert': [], 'lookup': [], 'remove': []}

    for _ in range(num_operations):
        key, value = generate_random_key_value()

        # Perform and measure each operation
        latencies['insert'].append(measure_operation_time(client, 'insert', key, value))
        latencies['lookup'].append(measure_operation_time(client, 'lookup', key, value))
        latencies['remove'].append(measure_operation_time(client, 'remove', key, value))

    # Calculating average latency and throughput
    avg_latency = {op: mean(times) for op, times in latencies.items()}
    throughput = {op: num_operations / (sum(times) / 1000) for op, times in latencies.items()}

    client.close()
    return avg_latency, throughput

def main():
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <MongoDB_URI>")
        sys.exit(1)

    mongo_uri = sys.argv[1]
    avg_latency, throughput = perform_test(mongo_uri)

    print("\nPerformance Metrics")
    print("===================")
    print("\nAverage Latency (milliseconds):")
    for operation, latency in avg_latency.items():
        print(f"  {operation.capitalize()}: {latency:.2f} ms")

    print("\nThroughput (operations/second):")
    for operation, ops in throughput.items():
        print(f"  {operation.capitalize()}: {ops:.2f} Ops/s")

if __name__ == "__main__":
    main()
