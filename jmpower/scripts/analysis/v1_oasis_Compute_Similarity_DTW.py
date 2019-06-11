import psycopg2
import datetime
import numpy as np
import multiprocessing
from io import StringIO
import time
from scipy.spatial.distance import cdist
import math

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
	sqlcursor.execute("SELECT resource_name, price_array FROM price_cache WHERE id = %s;", (nodeID,))
	result = sqlcursor.fetchall()

	resultDict = {}
	resultDict['resource_name'] = result[0][0]
	resultDict['price_array'] = result[0][1]

	return resultDict

def getBulkPriceData(minID, maxID, pgsqlconnection):
	
	sqlcursor = pgsqlconnection.cursor()
	sqlcursor.execute("SELECT resource_name, price_array FROM price_cache WHERE id >= %s AND id < %s;", (minID, maxID))
	results = sqlcursor.fetchall()
	
	resultList = []
	
	for result in results:
		resultDict = {}
		resultDict['resource_name'] = result[0]
		resultDict['price_array'] = result[1]
		resultList.append(resultDict.copy())
	
	return resultList

	
def findSimilarity(keyNode, totalNodes, chunkSize):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')
	sqlcursor = pgsqlconnection.cursor()

	keyNodeData = getPriceData(keyNode, pgsqlconnection)
	
	keyNodeName = keyNodeData['resource_name']

	corrList = []
	
	nodeCount = 1;
	
	while nodeCount < totalNodes:
		minID = nodeCount
		maxID = nodeCount + chunkSize

		nodes = getBulkPriceData(minID, maxID, pgsqlconnection)
		
		for node in nodes:
			nodeName = node['resource_name']
			initTime = time.time()
			corr = calculateDistance(keyNodeData['price_array'], node['price_array'])
			endTime = time.time()
			deltaTime = endTime - initTime
			print("Node A: " + keyNodeName + " Node B: " + nodeName + " Distance: " + str(corr) + " Time: " + str(deltaTime))
			storeResult(keyNodeName, nodeName, corr, pgsqlconnection)
		
		nodeCount = maxID

	pgsqlconnection.close()
	
	
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
	
	


def storeResult(resource_name, pair_name, similarity, pgsqlconnection):
	sqlcursor = pgsqlconnection.cursor()
	sqlcursor.execute("INSERT INTO dtw_sim (resource_name, pair_name, distance) VALUES(%s, %s, %s);",(resource_name, pair_name, similarity))
	pgsqlconnection.commit()


	

def similarityWorker(totalNode, chunkSize):
	run = True
	while run:	
		if nodeQueue.empty():
			run = False
		else:
			node = nodeQueue.get()
			findSimilarity(node, totalNodes, chunkSize)		
			
def createSimilarityTable():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("DROP TABLE IF EXISTS dtw_sim;") 

	sqlcursor.execute("CREATE TABLE dtw_sim (resource_name text, pair_name text, distance double precision);")
	
	pgsqlconnection.commit()
	pgsqlconnection.close()

if __name__ == "__main__":

	print("Counting Nodes...")
	
	totalNodes = getNodeCount()
	minNode = 1
	chunkSize = totalNodes
	maxNode = totalNodes
	
	createSimilarityTable()
	
	print (str(totalNodes) + " Nodes")
	
	nodeQueue = multiprocessing.Queue()
	
	for i in range(maxNode):
		nodeQueue.put(i+1)
	
	
	workers = []
	
	for i in range(4):
		p = multiprocessing.Process(target = similarityWorker, args=(totalNodes, chunkSize))
		workers.append(p)
		p.start()
		time.sleep(1)
	
	for p in workers:
		p.join()
		
	










