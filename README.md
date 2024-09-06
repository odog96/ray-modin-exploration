# ray-modin-exploration


#### Some basic set up commands

1. Install ray and boto3 locally
2. connect to aws cli
3. Staring Up: ray up modin-cluster.yaml
   Give it some time for the cluster to spin up. You can check by doing the following:
   - ray attach modin-cluster.yaml ( ssh connects you to head node)
   - ray status check the status of whole cluster. At the beginning you will so only partial cores and memory.
   - exit ( you need to exit to run submit commands via client)
4. Ray submit - this is how you run ray jobs:
   - ray submit modin-cluster.yaml exercise_5.py  
6. Don't Forget! When you're done bring down your cluster!
   use this command : Ray down modin-cluster.yaml

#### File description
- go_wide.py creates an initial wide data set by proving a 'widening factor'
- go_wider.py allows you to make the ouput from earlier file even wider using ray modin.
- pivot_ops.py does some pivoting operations on a handful of fields from the resultant dataset using ray modin.
