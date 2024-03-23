from pyspark import SparkContext, SparkConf
import time
import sys
import os


def MRApproxOutliers(inputPoints, D, M, K):
	# inputPoints: RDD already subdivided into a suitable number of partitions
	# D: radius of the circle
	# M: threshold of the number of points inside the circle
	# K: number of cells to print
	
    def calculateR3(cell):
        i, j = cell
        R3 = 0
        for j in range (i-1, i+2):
            for k in range (j-1, j+2):
                if (j,k) in cell_counts_dict:
                    R3 += cell_counts_dict[(j, k)]
        return R3
	
    def calculateR7(cell):
        i, j = cell
        R7 = 0
        for j in range (i-3, i+4):
            for k in range (j-3, j+4):
                if (j,k) in cell_counts_dict:
                    R7 += cell_counts_dict[(j, k)]
        return R7
		
    # contain, for each cell, its identifier (𝑖,𝑗) and the number of points of 𝑆 that it contains
    # Step A: Transform RDD into an RDD of non-empty cells
    cell_counts = (inputPoints.flatMap(lambda point: [((int(point[0] // D), int(point[1] // D)), 1)]) # <- MAP: each point, mapped to its corresponding cell identifier -> output: (cell_identifier, 1)
                        .reduceByKey(lambda x, y: x + y) # <- REDUCE: The pairs with the same cell identifier are grouped together and the values are summed up -> output: (cell_identifier, number of elements)
                        .filter(lambda cell_count: cell_count[1] > 0)) # filter non-empty cells
    
    sorted_cell_counts = cell_counts.sortBy(lambda x: x[1], ascending=False)
	
    print("First ", K, " elements: ")
    
    for element in sorted_cell_counts.take(K):
        print(element)
    
    cell_counts_dict = cell_counts.collectAsMap() # to make it a dictionary
    
	
    # Step B: attach to each element, relative to a non-empty cell 𝐶, the values |𝑁3(𝐶)| and |𝑁7(𝐶)|, as additional info
    cells_info =  cell_counts.flatMap(lambda cell: [(cell[0], cell[1], calculateR3(cell[0]), calculateR7(cell[0]))])
	
    # compute and print the number of sure outliers
    #cells_info_dict = cells_info.collectAsMap() # to make it a dictionary
    cells_info_list = cells_info.collect()
    outliers = 0
    uncertain = 0
    for cell in cells_info_list:
        if cell[3] <= M:
            outliers += 1
        elif cell[2] <= M & cell[3] > M:
            uncertain += 1
    
    print("Number of sure outliers =", outliers)
    print("Number of uncertain points =", uncertain)

def main():
	# To implement the algorithms assume that each point 𝑝 is represented through its coordinates (𝑥𝑝,𝑦𝑝), 
	# where each coordinate is a float, and that set 𝑆 is given in input as a file, where each row contains 
	# one point stored with the coordinates separated by comma (','). Assume also that all points are distinct.
    # CHECKING NUMBER OF CMD LINE PARAMTERS
	assert len(sys.argv) == 6, "Usage: python G034HW1.py <D> <M> <K> <L> <file_name>"

    # SPARK SETUP
	conf = SparkConf().setAppName('G034HW1')
	sc = SparkContext(conf=conf)

    # 1. Read the radius of the circle
	D = sys.argv[1]
	assert D.replace(".", "", 1).isdigit(), "D must be a float, with a point to separate the decimals"
	D = float(D)
	print("Radius of the circle =", D)
	
    # 2. Read the threshold to the number of points inside the circle
	M = sys.argv[2]
	assert M.isdigit(), "M must be an integer"
	M = int(M)
	print("Threshold to the number of points inside the circle =", M)
	
	# 3. Read the number of cells
	K = sys.argv[3]
	assert K.isdigit(), "K must be a integer"
	K = int(K)
	print("Number of cells = ", K)
	
	# 4. Read the number of partitions of the RDD of points
	L = sys.argv[4]
	assert L.isdigit(), "L must be a integer"
	L = int(L)
	print("Number of partitions of the RDD of points =", L)
	
    # 5. Read input file and subdivide it into L random partitions
	# The file contains the points represented through their coordinates (𝑥𝑝,𝑦𝑝)
	points_path = sys.argv[5]
	assert os.path.isfile(points_path), "File or folder not found"
	# create RDD and split into L partitions
	rawData = sc.textFile(points_path)
	inputPoints = rawData.map(lambda el: tuple(map(float, el.split(','))))
	numPoints = inputPoints.count()
	print("Number of points = ", numPoints)
	
	# .cache() -> keep it in memory, otherwise spark may decide to distruct it and keep only indications on how to reconstruct it
	inputPoints = inputPoints.repartition(L).cache()
	
	if numPoints <= 200000:
		listOfPoints = inputPoints.collect()
		# TODO: execute ExactOutliers with parameters listOfPoints, D, M and K
		# TODO: print running time of ExactOutliers
    
	startMRTime = time.time()
    
	MRApproxOutliers(inputPoints, D, M, K)
    
	endMRTime = time.time()
	runningTime = endMRTime - startMRTime
	print("Running time for MRApproxOutliers:", runningTime, "seconds")
	# TODO: print running time of MRApproxOutliers


if __name__ == "__main__":
	main()