import psycopg2
from geopy.distance import vincenty
from io import StringIO

	
def getNodeNames():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT resource_name FROM atlas WHERE lat IS NOT NULL AND long IS NOT NULL ORDER BY resource_name;")
	
	results = sqlcursor.fetchall()
	
	nodeNames = []

	for result in results:
		nodeNames.append(result[0])

		

	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	return nodeNames
	
def getNodeData():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT resource_name, lat, long FROM atlas WHERE lat IS NOT NULL AND long IS NOT NULL ORDER BY resource_name;")
	
	results = sqlcursor.fetchall()
	
	nodeData = {}

	for result in results:
		nodeData[result[0]] = result

		

	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	return nodeData

def getDistance(nodeA, nodeB, sqlcursor):

	nodeA_location = (nodeA[1], nodeA[2])
	nodeB_location = (nodeB[1], nodeB[2])
	
	distance = vincenty(nodeA_location, nodeB_location).km

	return distance
	

def storeResult(distanceList, sqlcursor):
	reportFile = StringIO()
	
	for dist in distanceList:
		reportLine = dist['resource_name'] + ',' + dist['pair_name'] + ',' + str(dist['distance']) + '\n'
		reportFile.write(reportLine)
	
	reportFile.seek(0)
	sqlcursor.copy_from(reportFile, 'physical_distance', columns=('resource_name', 'pair_name', 'distance_known'), sep=',')

def createPhysicalDistanceTable():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("DROP TABLE IF EXISTS physical_distance;") 

	sqlcursor.execute("CREATE TABLE physical_distance (resource_name text, pair_name text, distance_known double precision, distance_calculated double precision);")

	pgsqlconnection.commit()
	pgsqlconnection.close()



if __name__ == "__main__":
	
	createPhysicalDistanceTable()
	
	nodeNames = getNodeNames()
	
	nodeData = getNodeData()
	
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	
	
	for i in nodeNames:
		resultList = []
		for j in nodeNames:
			iData = nodeData[i]
			jData = nodeData[j]
			tempDic = {}
			tempDic['resource_name'] = i
			tempDic['pair_name'] = j
			tempDic['distance'] = getDistance(iData,jData, sqlcursor)
			resultList.append(tempDic.copy())
		
		storeResult(resultList, sqlcursor)
		pgsqlconnection.commit()

	

	pgsqlconnection.close()

	
	
	
	










