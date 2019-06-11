import psycopg2
import datetime
import numpy as np
import multiprocessing
import time
from scipy.spatial.distance import cdist
from numba import jit
import math
	
def getNodes(startDate, endDate, low_lat, high_lat, low_long, high_long):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT resource_name FROM atlas WHERE online <= %s::date AND offline >= %s::date AND lat BETWEEN %s AND %s AND long BETWEEN %s AND %s ORDER BY resource_name;", (startDate, endDate, low_lat, high_lat, low_long, high_long))

	idx = 0

	for result in sqlcursor.fetchall():
		sqlcursor.execute("INSERT INTO node_index_day (resource_name, index) VALUES (%s, %s);", (result[0], idx))
		idx = idx + 1
		
	
	pgsqlconnection.commit()
	pgsqlconnection.close()

def populateIntervalQueue(startDate, endDate):
	intervalQueue = multiprocessing.Queue()
	
	intermediaryDate = startDate
	
	while(intermediaryDate <= endDate):
		intervalQueue.put(intermediaryDate)
		intermediaryDate = intermediaryDate + datetime.timedelta(days=1)
	
	return intervalQueue
	

def generatePriceDict(interval, sqlcursor):
	
	priceDict = {}

	sqlcursor.execute("SELECT resource_name, array_agg(lmp_prc::numeric::double precision) FROM (SELECT resource_name, lmp_prc FROM lmp_rtm WHERE date = %s AND resource_name in (SELECT resource_name FROM node_index_day ORDER BY index) ORDER BY interval_start) AS price_data GROUP BY resource_name;", (interval,))

	for result in sqlcursor.fetchall():
		priceDict[result[0]] = result[1]

	return priceDict
		

def getNodeIndexes(sqlcursor):
	
	nodeIndexes = {}
	
	sqlcursor.execute("SELECT resource_name, index FROM node_index_day ORDER BY index")
	
	for result in sqlcursor.fetchall():
		nodeIndexes[result[0]] = result[1]
	
	return nodeIndexes
	
def generateSimilarityMatrix(node_indexes, price_dict):
	
	similarity_matrix = np.empty([len(node_indexes), len(node_indexes)])

	for i in node_indexes:
		for j in node_indexes:
			nodeA = price_dict[i]
			nodeB = price_dict[j]
			distance = calculateDistance(nodeA, nodeB)
			idx_a = node_indexes[i]
			idx_b = node_indexes[j] 
			similarity_matrix[idx_a][idx_b] = distance 
	
	return similarity_matrix
	
	
@jit	
def calculateDistance(nodeA, nodeB):

	xlen = len(nodeA)
	ylen = len(nodeB)
	
	x = np.array(nodeA).reshape(-1,1)
	y = np.array(nodeB).reshape(-1,1)

	dist_base = np.zeros((xlen + 1, ylen + 1))
	
	dist_base[0, 1:] = np.inf
	dist_base[1:, 0] = np.inf
	
	dist_view = dist_base[1:, 1:]
	
	dist_base[1:, 1:] = cdist(x, y, 'euclidean')
	
	for i in range(xlen):
		for j in range(ylen):
			dist_view[i, j] = dist_view[i,j] + min(dist_base[i,j], dist_base[i, j+1], dist_base[i+1,j])
	
	distance = dist_view[-1, -1]/math.sqrt(math.pow(dist_view.shape[0],2) + math.pow(dist_view.shape[1],2))
	
	return distance			
	
def storeSimilarityMatrix(interval, similarity_matrix, sqlcursor):

	matrix = similarity_matrix.tolist()

	sqlcursor.execute("INSERT INTO dtw_dist_day (day, distance_matrix) VALUES (%s, %s);",(interval,matrix))
		
	
	
def similarityWorker():
	
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	while not intervalQueue.empty():
		initTime = time.time()
		
		interval = intervalQueue.get()
		node_indexes = getNodeIndexes(sqlcursor)
		price_dict = generatePriceDict(interval, sqlcursor)
		similarity_matrix = generateSimilarityMatrix(node_indexes, price_dict)
		storeSimilarityMatrix(interval, similarity_matrix, sqlcursor)
		
		endTime = time.time()
		deltaTime = endTime - initTime
		print("Day: " + str(interval) + " Time: " + str(deltaTime))
		pgsqlconnection.commit()
		
	
	pgsqlconnection.close()



def createDayTable():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("DROP TABLE IF EXISTS dtw_dist_day;")
	
	sqlcursor.execute("CREATE TABLE dtw_dist_day (day date, distance_matrix double precision [][]);")
	
	sqlcursor.execute("DROP TABLE IF EXISTS node_index_day;") 

	sqlcursor.execute("CREATE TABLE node_index_day (resource_name text, index integer);")

	pgsqlconnection.commit()
	pgsqlconnection.close()
	


if __name__ == "__main__":
	
	startDate = datetime.date(2017, 1, 1)
	endDate = datetime.date(2017, 12, 31)
	
	low_lat = 32
	high_lat = 35
	low_long = -119
	high_long = -116
	
	createDayTable()
	
	nodes = getNodes(startDate, endDate, low_lat, high_lat, low_long, high_long)
	
# 	intervalQueue = populateIntervalQueue(startDate, endDate)
# 	
# 	totalWorkers = multiprocessing.cpu_count()
# 	
# 	workers = []
# 	
# 	for i in range(totalWorkers):
# 		p = multiprocessing.Process(target = similarityWorker)
# 		workers.append(p)
# 		p.start()
# 		time.sleep(1)
# 	
# 	for p in workers:
# 		p.join()
	
	
	
	
	










