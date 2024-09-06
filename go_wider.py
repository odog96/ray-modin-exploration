import time
import os
import ray
import modin.pandas as pd

import logging
logging.basicConfig(level=logging.DEBUG)


#ray.init(address="auto")
ray.init()
cpu_count = ray.cluster_resources()["CPU"]
assert cpu_count == 576, f"Expected 576 CPUs, but found {cpu_count}"

# file_path = "new_df.csv"
# df = pd.read_csv(file_path, quoting=3)

# import pandas as pd
file_path = "/home/ray/new_df.parquet/"
df = pd.read_parquet(file_path)
# print(df.head())



#file_path = "/home/ray/new_df.parquet/"


# Access the dataframe from any node

t0 = time.perf_counter()

# if not os.path.exists(file_path):
#     raise FileNotFoundError(f"The file {file_path} does not exist.")

#df = pd.read_parquet(file_path)

mult = 2 # widening factor

# this is 10k columns

# modin creation
start = time.time()
new_df = df
for i in range(mult):
    for col in df.columns:
        new_col_name = f"{col}_copy_{i}"
        new_df[new_col_name] = new_df[col]
end = time.time()
modin_duration = end - start
print("Time to concatination with Modin: {} seconds".format(round(modin_duration, 3)))

del(df)

width = new_df.shape[1]

new_df.to_parquet(f"new_df_{width}.parquet")

