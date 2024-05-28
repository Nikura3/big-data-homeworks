from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
from collections import defaultdict
import threading
import sys


def exactFrequentItems(stream):
    global candidates

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

    # Update global candidates
    for item, count in local_candidates.items():
        candidates[item] += count



if __name__ == '__main__':
    assert len(sys.argv) == 5, "USAGE: n, phi, epsilon, delta, portExp"

    # IMPORTANT: when running locally, it is *fundamental* that the
    # `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
    # there will be no processor running the streaming computation and your
    # code will crash with an out of memory (because the input keeps accumulating).
    conf = SparkConf().setMaster("local[*]").setAppName("G034HW3")
    # If you get an OutOfMemory error in the heap consider to increase the
    # executor and drivers heap space with the following lines:
    # conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)  # Batch duration of 0.01 seconds (stay under 1 seconds, the generator is fast)
    ssc.sparkContext.setLogLevel("ERROR")
    
    stopping_condition = threading.Event()
    
    
    # INPUT READING

    # n, phi, epsilon, delta, portExp 
    n = int(sys.argv[1])
    print("Number of items of the stream to be processed =", n)

    phi = float(sys.argv[2])
    print("The frequency thresold in (0,1) =", phi)

    epsilon = float(sys.argv[3])
    print("The accuracy parameter in (0,1) =", epsilon)

    delta = float(sys.argv[4])
    print("The confidence parameter in (0,1) =", delta)

    portExp = int(sys.argv[5])
    print("Receiving data from port =", portExp)


    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    candidates = defaultdict(int)
    k = int(1 / phi)
    

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)

    # For each batch, compute:
    #   - true frequent items with respect to the threshold phi
    #   - An m-sample of Σ using Reservoir Sampling of, with 𝑚 = ⌈1/𝑝ℎ𝑖⌉
    #   - The epsilon-Approximate Frequent Items computed using Sticky Sampling with confidence parameter delta

    # true frequent items with respect to the threshold phi
    stream.foreachRDD(lambda time, batch: exactFrequentItems(batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    ssc.stop(False, True)
    print("Streaming engine stopped")

    # COMPUTE AND PRINT FINAL STATISTICS
    print("Number of items processed =", streamLength[0])
    print("Number of distinct items =", len(histogram))
    largest_item = max(histogram.keys())
    print("Largest item =", largest_item)
    
