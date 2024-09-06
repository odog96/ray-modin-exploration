import time

import ray

import modin.pandas as pd

ray.init(address="auto")
cpu_count = ray.cluster_resources()["CPU"]
assert cpu_count == 576, f"Expected 576 CPUs, but found {cpu_count}"

file_path = "big_yellow.csv"

t0 = time.perf_counter()


df = pd.read_csv(file_path, quoting=3)
# df_count = df.count()
# df_groupby_count = df.groupby("passenger_count").count()
# df_map = df.map(str)


n_rows = 100000
row_mult = 1000 # widening factor

# this is 10k columns

# modin creation
start = time.time()
new_df = df.head(n_rows)
for i in range(row_mult):
    for col in df.columns:
        new_col_name = f"{col}_copy_{i}"
        new_df[new_col_name] = new_df[col]
end = time.time()
modin_duration = end - start
print("Time to concatination with Modin: {} seconds".format(round(modin_duration, 3)))


del(df)


new_df.write_csv("new_df.csv")

# pivot command 
start_time = time.time()
modin_pivot = new_df.pivot_table(index='PULocationID', values=['trip_distance', 'total_amount'], aggfunc='sum')
modin_duration = time.time() - start_time
print(f"pivot operation took {modin_duration:.4f} seconds")

t1 = time.perf_counter()
print(f"df shape is{new_df.shape}\n")
print(f"Full script time is {(t1 - t0):.3f}")  # noqa: T201
