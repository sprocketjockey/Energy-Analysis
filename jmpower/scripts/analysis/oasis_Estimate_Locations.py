import psycopg2
import datetime
import numpy as np
import threading
import queue



def getAllNodeNames(startDate, endDate):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

# 	sqlcursor.execute("SELECT DISTINCT(resource_name) FROM lmp_rtm WHERE date BETWEEN %s AND %s ORDER BY resource_name;", (startDate, endDate))
	
	sqlcursor.execute("SELECT resource_name FROM atlas WHERE online <= %s AND offline >= %s ORDER BY resource_name;", (startDate, endDate))
	
	results = sqlcursor.fetchall()
	
	nodeNames = {}
	counter = 0
	for result in results:
		nodeNames[result[0]] = counter
		counter += 1
		

	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	return nodeNames


def getKnownNodeNames(startDate, endDate):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT resource_name FROM atlas WHERE lat IS NOT NULL AND long IS NOT NULL AND resource_name IN (SELECT DISTINCT (resource_name) FROM lmp_rtm WHERE date BETWEEN %s AND %s) ORDER BY resource_name;", (startDate, endDate))
	
	results = sqlcursor.fetchall()
	


	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	return results

def getIntervals(startDate, endDate):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT DISTINCT(interval_start) FROM lmp_rtm WHERE date BETWEEN %s AND %s ORDER BY interval_start;", (startDate, endDate))
	
	results = sqlcursor.fetchall()
	
	intervals = {}
	counter = 0
	for result in results:
		intervals[result[0]] = counter
		counter += 1
		


	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	return intervals

def getPriceData(nodeName, startDate, endDate, sqlcursor):

	sqlcursor.execute("SELECT resource_name, interval_start, lmp_prc FROM lmp_rtm WHERE resource_name = %s AND date BETWEEN %s AND %s ORDER BY interval_start;", (nodeName,startDate, endDate))
	
	results = sqlcursor.fetchall()

	return results
	
def populateDataMatrix(dataMatrix, nodeNames, intervals, startDate, endDate):
	
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')
	sqlcursor = pgsqlconnection.cursor()
	
	while not nodeQueue.empty():
		node =  nodeQueue.get()
		priceRecords = getPriceData(node, startDate, endDate, sqlcursor)
		
		for price in priceRecords:
# 			print(str(nodeNames[price[0]]) + " " + str(intervals[price[1]]) + " " + str(price[2]))
			x = nodeNames[price[0]]
			y = intervals[price[1]]
			data = price[2].replace("$","")
			data = data.replace(",","")
			data = float(data)
			
			dataMatrix[x,y] = data
		
	
		print("Completed: " + node)
	pgsqlconnection.commit()
	pgsqlconnection.close()
		
		

def generateNodeQueue(nodeNames):

	nodeQueue = queue.Queue()
	
	for node in nodeNames.keys():
		nodeQueue.put(node)

	return nodeQueue

if __name__ == "__main__":
	
	
	startDate = datetime.date(2017, 7, 1)
	endDate = datetime.date(2017, 12, 25)
	
	print(startDate)
	print(endDate)
	nodeNames = getAllNodeNames(startDate, endDate)
	intervals = getIntervals(startDate, endDate)
	
	print (len(nodeNames))
	print (len(intervals))
	
	dataMatrix = np.zeros((len(nodeNames), len(intervals)))
	
	nodeQueue = generateNodeQueue(nodeNames)

	workers = []
	
	for i in range(8):
		t = threading.Thread(target = populateDataMatrix, args=(dataMatrix, nodeNames, intervals, startDate, endDate))
		workers.append(t)
		t.start()
	
	for t in workers:
		t.join()
	
	print("Saving to file.")
	np.savetxt("../../results/6months.csv", dataMatrix, delimiter=",")
	
	
	
# 	for k,v in nodeNames.items():
# 		print("Node: " + k + " Index: " + str(v))
#  	
# 	for k,v in intervals.items():
# 		print("Node: " + k.isoformat() + " Index: " + str(v))	