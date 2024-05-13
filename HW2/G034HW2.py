from pyspark import SparkContext, SparkConf
import time
import sys
import os
import math
from itertools import combinations
	
# SPARK SETUP
conf = SparkConf().setAppName('G034HW2')
sc = SparkContext(conf=conf)
conf.set("spark.locality.wait", "0s")

def squaredDistance(p1, p2):
    t0 = p1[0] - p2[0]
    t1 = p1[1] - p2[1]                                 
    return (t0 * t0 + t1 * t1)

def MRApproxOutliers(inputPoints, D, M):
	# inputPoints: RDD already subdivided into a suitable number of partitions
	# D: radius of the circle
	# M: threshold of the number of points inside the circle
	
    def calculateR3(cell):
        i, j = cell
        R3 = 0
        for x in range (i-1, i+2):
            for k in range (j-1, j+2):
                if (x,k) in cell_counts_dict:
                    R3 += cell_counts_dict[(x, k)]
        return R3
	
    def calculateR7(cell):
        i, j = cell
        R7 = 0
        for x in range (i-3, i+4):
            for k in range (j-3, j+4):
                if (x,k) in cell_counts_dict:
                    R7 += cell_counts_dict[(x, k)]
        return R7
		
    # contain, for each cell, its identifier (𝑖,𝑗) and the number of points of 𝑆 that it contains
    # Step A: Transform RDD into an RDD of non-empty cells
    cell_counts = (inputPoints.map(lambda point: ((int(point[0] // (D / (2*math.sqrt(2)))), int(point[1] // (D / (2*math.sqrt(2))))), 1)) # <- MAP: each point, mapped to its corresponding cell identifier -> output: (cell_identifier, 1)
                        .reduceByKey(lambda x, y: x + y )) # <- REDUCE: The pairs with the same cell identifier are grouped together and the values are summed up -> output: (cell_identifier, number of elements)

    # Step B: attach to each element, relative to a non-empty cell 𝐶, the values |𝑁3(𝐶)| and |𝑁7(𝐶)|, as additional info
    cell_counts_dict = cell_counts.collectAsMap() # to make it a dictionary
    cells_info = cell_counts.map(lambda cell: (cell[0], cell[1], calculateR3(cell[0]), calculateR7(cell[0])))
	
    # compute and print the number of sure outliers
    cells_info_list = cells_info.collect()
    outliers = 0
    uncertain = 0
    for cell, size, n3, n7 in cells_info_list:
        if n7 <= M:
            outliers += size
        elif n3 <= M and n7 > M:
            uncertain += size
    
    print("Number of sure outliers =", outliers)
    print("Number of uncertain points =", uncertain)


def SequentialFFT(inputPoints, K):
    # inputPoints : list of points
    # K: number of clusters
    # return a list C of K centers

    # Initialize the set of centers with the first point in inputPoints
    C = [inputPoints[0]] 
    
    # Use a set to store the points that are already in C
    C_set = {inputPoints[0]}
    
    # Use a set to store the points that are not in C
    input_set = set(inputPoints[1:])
    
    # Use a set to store the minimum distance of each point to the center
    min_distances = {point: squaredDistance(point, C[0]) for point in input_set}

    while len(C) < K: # while there's still points to collect
        farthest_point = None
        max_distance = -1
        
        for point in input_set:
            radius = min_distances[point]
            
            if radius > max_distance:
                max_distance = radius
                farthest_point = point
        
        C.append(farthest_point)
        C_set.add(farthest_point)
        input_set.remove(farthest_point)
        
        # Update the minimum distance of each point to the new center
        for point in input_set:
            distance = squaredDistance(point, farthest_point)
            if distance < min_distances.get(point, float('inf')):
                min_distances[point] = distance
        
    return C

def MRFFT(P, K):
    # P: input points stored in a RDD partitioned into L partitions
    # K: number of centers

    # R1
    startR1Time = time.time()
    # run SequentialFFT for each partition
    R1_RDD = P.mapPartitions(lambda partition: SequentialFFT(list(partition), K))  # L * K points
    R1_RDD.cache()
    R1_RDD.count()
    endR1Time = time.time()
    runningR1Time = endR1Time - startR1Time

    print("Running time of MRFFT Round 1 =", "{:.0f}".format(runningR1Time * 1000), "ms")
    
    # R2
    startR2Time = time.time()
    R1_RDD_collected = R1_RDD.collect()
    # apply SequentialFTT on the coreset
    C = SequentialFFT(R1_RDD_collected, K) # K points
    endR2Time = time.time()
    runningR2Time = endR2Time - startR2Time

    print("Running time of MRFFT Round 2 =", "{:.0f}".format(runningR2Time * 1000), "ms")
    
    broadcast_C = sc.broadcast(C)
    
    # R3
    startR3Time = time.time()
    # compute the maximum distance from each point to its nearest center
    radius = P.map(lambda point: math.sqrt(min(squaredDistance(point, center) for center in broadcast_C.value))).reduce(max)

    endR3Time = time.time()
    runningR3Time = endR3Time - startR3Time

    print("Running time of MRFFT Round 3 =", "{:.0f}".format(runningR3Time * 1000), "ms")
    return radius
    

def main():
    assert len(sys.argv) == 5, "Usage: python G034HW2.py <file_name> <M> <K> <L> "

    # 1. Read input file
    # The file contains the points represented through their coordinates (𝑥𝑝,𝑦𝑝)
    points_path = sys.argv[1]
    # assert os.path.isfile(points_path), "File or folder not found"

    # 2. Read the threshold to the number of points inside the circle
    M = sys.argv[2]
    assert M.isdigit(), "M must be an integer"
    M = int(M)

    # 3. Read the number of centers
    K = sys.argv[3]
    assert K.isdigit(), "K must be a integer"
    K = int(K)

    # 4. Read the number of partitions of the RDD of points
    L = sys.argv[4]
    assert L.isdigit(), "L must be a integer"
    L = int(L)

    print(sys.argv[1], " M=", M, " K=", K, " L=", L, sep='')

    # create RDD and split into L partitions
    rawData = sc.textFile(points_path)
    inputPoints = rawData.map(lambda el: tuple(map(float, el.split(','))))
    numPoints = inputPoints.count()
    print("Number of points =", numPoints)

    inputPoints = inputPoints.repartition(L)

    D = MRFFT(inputPoints, K)
    print("Radius =", "{:.8f}".format(D))

    startMRApproxTime = time.time()
    MRApproxOutliers(inputPoints, D, M)
    endMRApproxTime = time.time()
    runningMRApproxTime = endMRApproxTime - startMRApproxTime

    print("Running time of MRApproxOutliers =", "{:.0f}".format(runningMRApproxTime * 1000), "ms")


if __name__ == "__main__":
	main()