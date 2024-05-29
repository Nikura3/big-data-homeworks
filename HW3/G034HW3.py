from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
from collections import defaultdict
import threading
import sys
import random
import math

def exactFrequentItems(stream, k, candidates, global_count, stopping_condition, n):
    # Convert RDD to a list of items
    items = stream.collect()

    # Keep a counter for each item
    local_candidates = defaultdict(int)

    for item in items:
        if item in local_candidates:
            local_candidates[item] += 1
        elif len(local_candidates) < k:
            local_candidates[item] = 1
        else:
            for key in list(local_candidates.keys()):
                local_candidates[key] -= 1
                if local_candidates[key] == 0:
                    del local_candidates[key]

    # Update global candidates and global count
    for item, count in local_candidates.items():
        candidates[item] += count
        global_count[0] += count

    # Check if the total number of processed items meets or exceeds n
    if global_count[0] >= n:
        stopping_condition.set()

def reservoirSampling(stream, sample, m, global_count):
    # Convert RDD to a list of items
    items = stream.collect()

    for item in items:
        global_count[0] += 1
        if len(sample) < m:
            sample.append(item)
        else:
            s = random.randint(0, global_count[0] - 1)
            if s < m:
                sample[s] = item

if __name__ == '__main__':
    assert len(sys.argv) == 6, "USAGE: n, phi, epsilon, delta, portExp"

    # Spark Configuration
    conf = SparkConf().setMaster("local[*]").setAppName("G034HW3")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)  # Batch duration of 0.01 seconds
    ssc.sparkContext.setLogLevel("ERROR")
    
    stopping_condition = threading.Event()

    # Input Parameters
    n = int(sys.argv[1])
    print("Number of items of the stream to be processed =", n)

    phi = float(sys.argv[2])
    print("The frequency threshold in (0,1) =", phi)

    epsilon = float(sys.argv[3])
    print("The accuracy parameter in (0,1) =", epsilon)

    delta = float(sys.argv[4])
    print("The confidence parameter in (0,1) =", delta)

    portExp = int(sys.argv[5])
    print("Receiving data from port =", portExp)

    # Data Structures
    candidates = defaultdict(int)
    global_count = [0]  # Use a list to allow modification within exactFrequentItems
    k = int(1 / phi)
    m = math.ceil(1 / phi)
    sample = []

    # Stream Processing
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)

    stream.foreachRDD(lambda time, rdd: exactFrequentItems(rdd, k, candidates, global_count, stopping_condition, n))
    stream.foreachRDD(lambda time, rdd: reservoirSampling(rdd, sample, m, global_count))
    
    # Managing Streaming Context
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    ssc.stop(False, True)
    print("Streaming engine stopped")

    # Compute and Print Final Statistics
    print("Number of items processed =", global_count[0])
    print("Number of distinct items =", len(candidates))
    if candidates:
        largest_item = max(candidates.keys(), key=(lambda key: candidates[key]))
        print("Largest item =", largest_item)
    print("Reservoir sample size =", len(sample))
    print("Sample items =", sample)
