from pyspark import SparkContext, SparkConf
import time
import sys
import os
import math
from itertools import combinations
	
# SPARK SETUP
conf = SparkConf().setAppName('G034HW2')
sc = SparkContext(conf=conf)

#ho messo la distanza dello scorso hw invece che la euclidian
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


def SequentialFTT(inputPoints, K):
    #inputPoints(list)
    #K: number of clusters
    #return a set C (list) of K centers
    #the farthest-first traversal is a sequence of points in the space, where the first point 
    #is selected arbitrarily and each successive point is as far as possible from the set of previously-selected points.

    C = [inputPoints[0]]    #inizialize the set of centers with the first point in P
    while len(C) < K: #while there's still points to "collect"
        farthest_point = None
        max_distance = -1
        for point in inputPoints:
            #calculate the minimum distance from point to the current set of centers C
            min_distance = min(squaredDistance(point, center) for center in C)
            if min_distance > max_distance:
                max_distance = min_distance
                farthest_point = point
        C.append(farthest_point)
    return C


def maxDistance(max_distance, point):
    # Compute the maximum distance between the point and the accumulated maximum distance
    return max(max_distance, point)


def MRFFT(P, K):
    # P: input points stored in a RDD partitioned into L partitions
    # K: number of centers

    # run SequentialFFT for each partition
    R1_RDD = P.mapPartitions(lambda partition: [SequentialFTT(list(partition), K)])
    
    # collect results of R1_RDD
    R1_result = R1_RDD.collect()
    
    # apply SequentialFTT on the coreset
    C = SequentialFTT(R1_result, K)
    broadcast_C = sc.broadcast(C)
    
    # compute the maximum distance from each point to its nearest center
    #max_distance = P.map(lambda point: max([squaredDistance(point, center) for center in broadcast_C.value])).max()
    first_3_lines = P.take(3)
    for line in first_3_lines:
        print(line)
    # usando reduce come suggerito da loro
    max_distance = P.map(lambda point: max([squaredDistance(point, center) for center in C])).reduce(maxDistance)
    

def main():
    assert len(sys.argv) == 5, "Usage: python G034HW2.py <file_name> <M> <K> <L> "

    # 1. Read input file
    # The file contains the points represented through their coordinates (𝑥𝑝,𝑦𝑝)
    points_path = sys.argv[1]
    assert os.path.isfile(points_path), "File or folder not found"

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

    MRFFT(inputPoints, K)



if __name__ == "__main__":
	main()