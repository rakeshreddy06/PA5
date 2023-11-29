import time
import random
import string
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
def random_string(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))


import sys

# Connect to Cassandra
if len(sys.argv) > 1:
    contact_point = sys.argv[1]
else:
    print("Usage: python3 script_name.py <CASSANDRA_IP>")
    sys.exit(1)

# Replace with your own keyspace
KEYSPACE = "mykeyspace"

# Connect to Cassandra
#cluster = Cluster(['172.25.0.2'])
cluster = Cluster(
    contact_points=[contact_point],
    load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())
)
session = cluster.connect(KEYSPACE)

# Ensure the table exists (adjust the schema as necessary)
session.execute("""
CREATE TABLE IF NOT EXISTS my_table (
    key text PRIMARY KEY,
    value text
)
""")

# Prepare statements
insert_stmt = session.prepare("INSERT INTO my_table (key, value) VALUES (?, ?)")
select_stmt = session.prepare("SELECT value FROM my_table WHERE key = ?")
delete_stmt = session.prepare("DELETE FROM my_table WHERE key = ?")

num_operations = 100000  # Number of operations for testing
insert_latencies = []
lookup_latencies = []
delete_latencies = []

# Generate test data
test_data = [(random_string(10), random_string(90)) for _ in range(num_operations)]

# Insert operations
for key, value in test_data:
    start_time = time.time()
    session.execute(insert_stmt, (key, value))
    end_time = time.time()
    insert_latencies.append((end_time - start_time) * 1000)  # Convert to milliseconds

# Lookup operations
for key, _ in test_data:
    start_time = time.time()
    session.execute(select_stmt, [key])
    end_time = time.time()
    lookup_latencies.append((end_time - start_time) * 1000)

# Delete operations
for key, _ in test_data:
    start_time = time.time()
    session.execute(delete_stmt, [key])
    end_time = time.time()
    delete_latencies.append((end_time - start_time) * 1000)

# Calculate average latency and throughput for each operation
def calculate_metrics(latencies):
    avg_latency = sum(latencies) / len(latencies)
    throughput = num_operations / (sum(latencies) / 1000)  # Operations per second
    return avg_latency, throughput

avg_insert_latency, insert_throughput = calculate_metrics(insert_latencies)
avg_lookup_latency, lookup_throughput = calculate_metrics(lookup_latencies)
avg_delete_latency, delete_throughput = calculate_metrics(delete_latencies)

print(f"Insert - Average Latency: {avg_insert_latency:.2f} ms, Throughput: {insert_throughput:.2f} Ops/s")
print(f"Lookup - Average Latency: {avg_lookup_latency:.2f} ms, Throughput: {lookup_throughput:.2f} Ops/s")
print(f"Delete - Average Latency: {avg_delete_latency:.2f} ms, Throughput: {delete_throughput:.2f} Ops/s")