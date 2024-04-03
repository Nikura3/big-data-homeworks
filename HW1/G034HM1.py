from pyspark import SparkContext, SparkConf
import time
import sys
import os
import math


def ExactOutliers(inputPoints, D, M, K):
    # inputPoints: list of points 
    # D: threshold distance
    # M: minimum number of pomts in the circle
    # K: number of outliners to print
    
    def squaredDistance(p1, p2):                                    
        return ((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    outliers = []
    outlier_counts = 0
    

    #Calculate square distances for each point 
    for p1 in inputPoints:
        count = 1 # consider p1 in the count
        for p2 in inputPoints:
            if p1 != p2:                                                 
                if squaredDistance(p1,p2) <=D: #se facciamo con square root allora <=D**2, fare prove per vedere il migliore
                    count += 1
        if count <= M: 
                outlier_counts += 1
                outliers.append((p1, count))

	# Print number of outliers
    print("Number of Outliers =", outlier_counts)
    
	# Sort outliers by |BS(p, D)|
    outliers.sort(key=lambda x: x[1])
    
	# Print first K outliers in non-decreasing order of |BS(p, D)|
    for i in range(min(K, len(outliers))):
        print("Point:", outliers[i][0])



def MRApproxOutliers(inputPoints, D, M, K):
	# inputPoints: RDD already subdivided into a suitable number of partitions
	# D: radius of the circle
	# M: threshold of the number of points inside the circle
	# K: number of cells to print
	
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
    cell_counts = (inputPoints.map(lambda point: ((int(point[0] // (D/(2*math.sqrt(2)))), int(point[1] // (D/(2*math.sqrt(2))))), 1)) # <- MAP: each point, mapped to its corresponding cell identifier -> output: (cell_identifier, 1)
                        .reduceByKey(lambda x, y: x + y) # <- REDUCE: The pairs with the same cell identifier are grouped together and the values are summed up -> output: (cell_identifier, number of elements)
                        .filter(lambda cell_count: cell_count[1] > 0)) # filter non-empty cells
    
    # Step B: attach to each element, relative to a non-empty cell 𝐶, the values |𝑁3(𝐶)| and |𝑁7(𝐶)|, as additional info
    cell_counts_dict = cell_counts.collectAsMap() # to make it a dictionary
    cells_info = cell_counts.map(lambda cell: (cell[0], cell[1], calculateR3(cell[0]), calculateR7(cell[0])))
	
    # compute and print the number of sure outliers
    #cells_info_dict = cells_info.collectAsMap() # to make it a dictionary
    cells_info_list = cells_info.collect()
    outliers = 0
    uncertain = 0
    for cell, size, n3, n7 in cells_info_list:
        if n7 <= M:
            outliers += 1
        elif n3 <= M and n7 > M:
            uncertain += 1
    
    print("Number of sure outliers =", outliers)
    print("Number of uncertain points =", uncertain)

    sorted_cells = cell_counts.map(lambda x: (x[1], x[0])).sortByKey()
    first_K_cells = sorted_cells.take(K)

    # Print the identifier and the size of each cell
    for count, cell_id in first_K_cells:
        print("Cell:", cell_id, "Size:", count)
    
    
	

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

    print(sys.argv[5], " D=", D, " M=", M, " K=", K, " L=", L, sep='')

    # 5. Read input file and subdivide it into L random partitions
    # The file contains the points represented through their coordinates (𝑥𝑝,𝑦𝑝)
    points_path = sys.argv[5]
    assert os.path.isfile(points_path), "File or folder not found"
    # create RDD and split into L partitions
    rawData = sc.textFile(points_path)
    inputPoints = rawData.map(lambda el: tuple(map(float, el.split(','))))
    numPoints = inputPoints.count()
    print("Number of points =", numPoints)

    # .cache() -> keep it in memory, otherwise spark may decide to distruct it and keep only indications on how to reconstruct it
    inputPoints = inputPoints.repartition(L)

    if numPoints <= 200000:
        listOfPoints = inputPoints.collect()
        startETime = time.time()
        ExactOutliers(listOfPoints, D, M, K)
        endETime = time.time()
        runningETime = endETime - startETime
        print("Running time of ExactOutliers =", "{:.0f}".format(runningETime * 1000), "ms")

    startMRTime = time.time()

    MRApproxOutliers(inputPoints, D, M, K)

    endMRTime = time.time()
    runningTime = endMRTime - startMRTime
    print("Running time of MRApproxOutliers =", "{:.0f}".format(runningTime * 1000), "ms")
    # TODO: print running time of MRApproxOutliers



if __name__ == "__main__":
	main()