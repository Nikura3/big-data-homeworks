from pyspark import SparkContext, SparkConf
import time
import sys
import os
import math
from itertools import combinations
	

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
    

def main():
    assert len(sys.argv) == 6, "Usage: python G034HW2.py <file_name> <M> <K> <L> "

    # SPARK SETUP
    conf = SparkConf().setAppName('G034HW2')
    sc = SparkContext(conf=conf)

    # 1. Read input file
    # The file contains the points represented through their coordinates (𝑥𝑝,𝑦𝑝)
    points_path = sys.argv[1]
    assert os.path.isfile(points_path), "File or folder not found"

    # 2. Read the threshold to the number of points inside the circle
    M = sys.argv[2]
    assert M.isdigit(), "M must be an integer"
    M = int(M)

    # 3. Read the number of cells to print
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



if __name__ == "__main__":
	main()