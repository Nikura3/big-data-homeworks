from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
from collections import defaultdict
import threading
import sys
import random
import math

def exactCount(stream, candidates, global_count, stopping_condition, n):
    items = stream.collect()

    for item in items:
        candidates[item] += 1
        global_count[0] += 1

    if global_count[0] >= n:
        stopping_condition.set()


def reservoirSampling(stream, reservoir, m, t):
    items = stream.collect()
    for item in items:
        t[0] += 1
        if len(reservoir) < m:
            reservoir.append(item)
        else:
            s = random.randint(0, t[0] - 1)
            if s < m:
                reservoir[s] = item

def stickySampling(stream, hash_table, hash_table_size, phi, delta):
    for item in stream.collect():
        h = hash(item) % hash_table_size
        if hash_table[h] == item:
            continue
        x = random.random()
        if x <= phi:
            if random.random() < delta:
                hash_table[h] = item

if __name__ == '__main__':
    assert len(sys.argv) == 6, "USAGE: n, phi, epsilon, delta, portExp"

    conf = SparkConf().setMaster("local[*]").setAppName("G034HW3")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event()
    lock = threading.Lock()

    n = int(sys.argv[1])
    phi = float(sys.argv[2])
    epsilon = float(sys.argv[3])
    delta = float(sys.argv[4])
    portExp = int(sys.argv[5])

    print("INPUT PROPERTIES")
    print(f"n = {n} phi = {phi} epsilon = {epsilon} delta = {delta} port = {portExp}")

    candidates = defaultdict(int)
    global_count = [0]
    t = [0]
    m = math.ceil(1 / phi)
    reservoir = []
    hash_table_size = 10000
    hash_table = defaultdict(lambda: 0)

    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)

    stream.foreachRDD(lambda time, rdd: exactCount(rdd, candidates, global_count, stopping_condition, n))
    stream.foreachRDD(lambda time, rdd: reservoirSampling(rdd, reservoir, m, t))
    # stream.foreachRDD(lambda time, rdd: stickySampling(rdd, hash_table, hash_table_size, phi, delta))

    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, True)

    true_frequent_items = {item for item, count in candidates.items() if count >= phi * n}
    estimated_frequent_items_reservoir = {item for item in reservoir}
    # estimated_frequent_items_sticky = {item for item in hash_table.values() if item != 0}

    # Output for EXACT ALGORITHM
    print("EXACT ALGORITHM")
    print(f"Number of items in the data structure = {len(candidates)}")
    print(f"Number of true frequent items = {len(true_frequent_items)}")
    print("True frequent items:")
    # Sort the numbers in increasing order
    for item in sorted(true_frequent_items, key=lambda x: int(x)):
        print(item)


    # Output for RESERVOIR SAMPLING
    print("RESERVOIR SAMPLING")
    print(f"Size m of the sample = {m}")
    print(f"Number of estimated frequent items = {len(estimated_frequent_items_reservoir)}")
    print("Estimated frequent items:")
    for item in sorted(estimated_frequent_items_reservoir, key=lambda x: int(x)):
        sign = "+" if item in true_frequent_items else "-"
        print(f"{item} {sign}")

    # Output for STICKY SAMPLING
    # print("STICKY SAMPLING")
    # print(f"Number of items in the Hash Table = {len(hash_table)}")
    # print(f"Number of estimated frequent items = {len(estimated_frequent_items_sticky)}")
    # print("Estimated frequent items:")
    # for item in sorted(estimated_frequent_items_sticky):
    #     sign = "+" if item in true_frequent_items else "-"
    #     print(f"{item} {sign}")
