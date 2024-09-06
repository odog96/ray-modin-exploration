import time

import ray

import modin.pandas as pd

ray.init(address="auto")
cpu_count = ray.cluster_resources()["CPU"]
assert cpu_count == 576, f"Expected 576 CPUs, but found {cpu_count}"

file_path = "new_df.parquet"

t0 = time.perf_counter()

df = pd.read_parquet(file_path)

# # pivot command 
start_time = time.time()
modin_pivot = df.pivot_table(index='PULocationID', values=['trip_distance', 'total_amount'], aggfunc='sum')
modin_duration = time.time() - start_time
print(f"the shape is {df.shape}")
print(f"pivot operation took {modin_duration:.4f} seconds")

t1 = time.perf_counter()
print(f"Full script time is {(t1 - t0):.3f}")  # noqa: T201