import psycopg2
import datetime
import numpy as np
import multiprocessing
from io import StringIO
import time

def getNodeCount():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')
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
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')
	sqlcursor = pgsqlconnection.cursor()
	
	initTime = time.time()
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
			corr = calculateSimilarity(keyNodeData['price_array'], node['price_array'], pgsqlconnection)
			resultDict = {}
			resultDict['resource_name'] = keyNodeName
			resultDict['pair_name'] = nodeName
			resultDict['similarity'] = corr
			corrList.append(resultDict.copy())
		
		nodeCount = maxID

	storeResult(corrList, sqlcursor)
	pgsqlconnection.commit()
	pgsqlconnection.close()
	endTime = time.time()
	deltaTime = endTime - initTime
	print("Node: " + keyNodeName + " Time: " + str(deltaTime)) 
	
def calculateSimilarity(nodeA, nodeB, pgsqlconnection):
	
	sqlcursor = pgsqlconnection.cursor()

	npVector_a = np.array(nodeA)
	npVector_b = np.array(nodeB)

	corr = np.corrcoef(npVector_a, npVector_b)[0][1]
	
	return corr


def storeResult(corrList, sqlcursor):

	reportFile = StringIO()
	
	for corr in corrList:
		reportLine = corr['resource_name'] + ',' + corr['pair_name'] + ',' + str(corr['similarity']) + '\n'
		reportFile.write(reportLine)
	
	reportFile.seek(0)
	sqlcursor.copy_from(reportFile, 'cosign_sim', columns=('resource_name', 'pair_name', 'similarity'), sep=',')
	

def similarityWorker(totalNode, chunkSize):
	run = True
	while run:	
		if nodeQueue.empty():
			run = False
		else:
			node = nodeQueue.get()
			findSimilarity(node, totalNodes, chunkSize)		
			
def createSimilarityTable():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("DROP TABLE IF EXISTS cosign_sim;") 

	sqlcursor.execute("CREATE TABLE cosign_sim (resource_name text, pair_name text, similarity double precision);")
	
	pgsqlconnection.commit()
	pgsqlconnection.close()

if __name__ == "__main__":

	print("Counting Nodes...")
	
	totalNodes = getNodeCount()
	minNode = 1
	chunkSize = 100
	maxNode = totalNodes
	
	createSimilarityTable()
	
	print (str(totalNodes) + " Nodes")
	
	nodeQueue = multiprocessing.Queue()
	
	for i in range(maxNode):
		nodeQueue.put(i+1)
	
	
	workers = []
	
	for i in range(8):
		p = multiprocessing.Process(target = similarityWorker, args=(totalNodes, chunkSize))
		workers.append(p)
		p.start()
		time.sleep(1)
	
	for p in workers:
		p.join()
		

	
	
	
	










