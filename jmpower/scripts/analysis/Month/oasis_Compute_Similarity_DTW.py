import psycopg2
import datetime
import numpy as np
import multiprocessing
from io import StringIO
import time
from scipy.spatial.distance import cdist
import math
from numba import jit

def getNodeCount():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')
	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("SELECT COUNT(*) FROM price_cache;")
	
	count = sqlcursor.fetchall()[0][0]
	
	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	return count
	
def getPriceData(nodeID, pgsqlconnection):
	
	sqlcursor = pgsqlconnection.cursor()
	sqlcursor.execute("SELECT price_array FROM price_cache WHERE resource_name = %s;", (nodeID,))
	result = sqlcursor.fetchall()

	return result[0][0]
	
		
	
def findSimilarity(pair, id, pgsqlconnection):
	
	nodeA = pair[0]
	nodeB = pair[1]
	
	nodeA_price = getPriceData(nodeA, pgsqlconnection)
	nodeB_price = getPriceData(nodeB, pgsqlconnection)

	initTime = time.time()
	dist = calculateDistance(nodeA_price, nodeB_price)
	endTime = time.time()
	deltaTime = endTime - initTime
	print("Node A: " + nodeA + " Node B: " + nodeB + " Distance: " + str(dist) + " Time: " + str(deltaTime))
	storeResult(id, dist, pgsqlconnection)

	
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
	

def storeResult(id, dist, pgsqlconnection):
	sqlcursor = pgsqlconnection.cursor()
	sqlcursor.execute("UPDATE dtw_dist SET distance = %s WHERE id = %s;",(dist, id))
	pgsqlconnection.commit()


def similarityWorker(threadNumber, totalWorkers):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')
	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("SELECT resource_name, pair_name, id FROM dtw_dist WHERE id %% %s = %s ORDER BY id;", (totalWorkers, threadNumber))
	
	results = sqlcursor.fetchall()
	
	nodeList = {}
	
	for result in results:
		if result[2] not in nodeList:
			pair = (result[0], result[1])
			nodeList[result[2]] = pair
		
	for node,pair in nodeList.items():
		findSimilarity(pair, node, pgsqlconnection)
			
	
			
def createDistanceTable():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("DROP TABLE IF EXISTS dtw_dist;") 

	sqlcursor.execute("CREATE TABLE dtw_dist (resource_name text, pair_name text, distance double precision, id integer);")
	
	sqlcursor.execute("SELECT resource_name FROM price_cache;")
	
	results = sqlcursor.fetchall()
	
	nodeNames = []
	
	for result in results:
		nodeNames.append(result[0])
		
	nodeCount = len(nodeNames)
	
	id = 0
	
	resultList = []
	
	for i in range(0, nodeCount):
		for j in range(i, nodeCount):
			dictA = {}
			dictA['resource_name'] = nodeNames[i]
			dictA['pair_name'] = nodeNames[j]
			dictA['id'] = id
			dictB = {}
			dictB['resource_name'] = nodeNames[j]
			dictB['pair_name'] = nodeNames[i]
			dictB['id'] = id
			resultList.append(dictA.copy())
			resultList.append(dictB.copy())
			id = id + 1
			
	storeDistanceTable(resultList, sqlcursor)
	
	pgsqlconnection.commit()
	pgsqlconnection.close()


def storeDistanceTable(distanceList, sqlcursor):
	reportFile = StringIO()
	
	for dist in distanceList:
		reportLine = dist['resource_name'] + ',' + dist['pair_name'] + ',' + str(dist['id']) + '\n'
		reportFile.write(reportLine)
	
	reportFile.seek(0)
	sqlcursor.copy_from(reportFile, 'dtw_dist', columns=('resource_name', 'pair_name', 'id'), sep=',')


if __name__ == "__main__":

	print("Counting Nodes...")
	
	totalNodes = getNodeCount()
	minNode = 1
	chunkSize = totalNodes
	maxNode = totalNodes
	
	createDistanceTable()
	
	print (str(totalNodes) + " Nodes")
	
	nodeQueue = multiprocessing.Queue()
	
	for i in range(maxNode):
		nodeQueue.put(i+1)
	
	totalWorkers = multiprocessing.cpu_count()
	
	workers = []
	
	for i in range(totalWorkers):
		p = multiprocessing.Process(target = similarityWorker, args=(i, totalWorkers))
		workers.append(p)
		p.start()
		time.sleep(1)
	
	for p in workers:
		p.join()
		
	










