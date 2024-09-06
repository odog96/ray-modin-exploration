import ray

# Initialize Ray (connect to an existing Ray cluster)
ray.init(address='auto')

# Retrieve Ray cluster information
cluster_resources = ray.cluster_resources()
node_ip_address = ray.get(ray.nodes()[0])["NodeManagerAddress"]

# Construct the dashboard URL
dashboard_url = f"http://{node_ip_address}:8265"
print("Ray Dashboard URL:", dashboard_url)
