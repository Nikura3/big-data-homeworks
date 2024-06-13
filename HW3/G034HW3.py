from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
from collections import defaultdict
import threading
import sys
import random
import math

def exactCount(rdd, candidates, global_count, stopping_condition, n):
    items = rdd.collect()
    for item in items:
        candidates[item] += 1
        global_count[0] += 1
        if global_count[0] >= n:
            stopping_condition.set()
            return

def reservoirSampling(rdd, reservoir, m, t, n):
    items = rdd.collect()
    for item in items:
        t[0] += 1
        if len(reservoir) < m:
            reservoir.append(item)
        else:
            s = random.randint(0, t[0] - 1)
            if s < m:
                reservoir[s] = item
        if t[0] >= n:
            return

def stickySampling(rdd, hash_table, hash_table_size, item_count, n):
    items = rdd.collect()
    for item in items:
        item_count[0] += 1
        if item in hash_table:
            hash_table[item] += 1
        else:
            if random.random() <= hash_table_size / n:
                hash_table[item] = 1
        if item_count[0] >= n:
            return

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
    item_count = [0]
    hash_table_size = (1 / epsilon) * math.log(1 / (delta * phi))
    hash_table = defaultdict(int)

    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)

    stream.foreachRDD(lambda time, rdd: exactCount(rdd, candidates, global_count, stopping_condition, n))
    stream.foreachRDD(lambda time, rdd: reservoirSampling(rdd, reservoir, m, t, n))
    stream.foreachRDD(lambda time, rdd: stickySampling(rdd, hash_table, hash_table_size, item_count, n))
    
    def stop_streaming(ssc, stopping_condition):
        stopping_condition.wait()
        ssc.stop(False, True)
    
    stop_thread = threading.Thread(target=stop_streaming, args=(ssc, stopping_condition))
    stop_thread.start()

    ssc.start()
    stop_thread.join()

    true_frequent_items = {item for item, count in candidates.items() if count >= phi * n}
    estimated_frequent_items_reservoir = {item for item in reservoir}
    estimated_frequent_items_sticky = {item: count for item, count in hash_table.items() if count >= (phi - epsilon) * n}

    # Output for EXACT ALGORITHM
    print("EXACT ALGORITHM")
    print(f"Number of items in the data structure = {len(candidates)}")
    print(f"Number of true frequent items = {len(true_frequent_items)}")
    print("True frequent items:")
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
    print("STICKY SAMPLING")
    print(f"Number of items in the Hash Table = {len(hash_table)}")
    print(f"Number of estimated frequent items = {len(estimated_frequent_items_sticky)}")
    print("Estimated frequent items:")
    for item in sorted(estimated_frequent_items_sticky):
        sign = "+" if item in true_frequent_items else "-"
        print(f"{item} {sign}")
