import time
import ray

import modin.pandas as pd
import os

print("Current working directory:", os.getcwd())

ray.init(address="auto")
cpu_count = ray.cluster_resources()["CPU"]
assert cpu_count == 576, f"Expected 576 CPUs, but found {cpu_count}"

file_path = "big_yellow.csv"

df = pd.read_csv(file_path, quoting=3)
print('read data frame')
n_rows = 100000
mult = 100 # widening factor

# modin creation
start = time.time()
new_df = df.head(n_rows)

cols = df.columns
del(df)
# Determine checkpoints
checkpoints = [int(mult * 0.25), int(mult * 0.5), int(mult * 0.75)]

for i in range(mult):
    for col in cols:
        new_col_name = f"{col}_copy_{i}"
        new_df[new_col_name] = new_df[col]
    
    # Check and print progress
    if i == checkpoints[0]:
        print("25% done")
    elif i == checkpoints[1]:
        print("50% done")
    elif i == checkpoints[2]:
        print("75% done")
    
    #print('i is', i)

end = time.time()
modin_duration = end - start
print("Time to concatination with Modin: {} seconds".format(round(modin_duration, 3)))

print("shape of the output df is",new_df.shape)



new_df.to_parquet("new_df.parquet")
#new_df.to_csv("new_df.csv")

# # pivot command 
# start_time = time.time()
# modin_pivot = new_df.pivot_table(index='PULocationID', values=['trip_distance', 'total_amount'], aggfunc='sum')
# modin_duration = time.time() - start_time
# print(f"pivot operation took {modin_duration:.4f} seconds")

# t1 = time.perf_counter()
# print(f"df shape is{new_df.shape}\n")
# print(f"Full script time is {(t1 - t0):.3f}")  # noqa: T201
