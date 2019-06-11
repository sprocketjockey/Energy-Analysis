import psycopg2
import numpy as np


def generatePhysicalMatrix():

	node_index = getNodeIndex()
	
	dist_matrix = np.empty([len(node_index),len(node_index)])
	
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("SELECT resource_name, pair_name, distance_known FROM physical_distance WHERE resource_name IN (SELECT resource_name FROM node_index_day ORDER BY index) AND pair_name IN (SELECT resource_name FROM node_index_day ORDER BY index);")
	
	results = sqlcursor.fetchall()
	
	for result in results:
		x = node_index[result[0]]
		y = node_index[result[1]]
		dist = result[2]
		
		dist_matrix[x][y] = dist
		
	pgsqlconnection.commit()
	pgsqlconnection.close()

	return dist_matrix
	
def getNodeIndex():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT resource_name, index FROM node_index_day;")
	results = sqlcursor.fetchall()
	
	node_index = {}
	
	for result in results:
		node_index[result[0]] = result[1]
	
	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	return node_index
	
	

def generateDistMatrixList():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("SELECT distance_matrix FROM dtw_dist_day ORDER BY day;")
	
	results = sqlcursor.fetchall()
	
	dist_matrix_list = []
	
	for result in results:
		dist_matrix_list.append(np.array(result[0]))

	return dist_matrix_list

def calculateStressMatrix(phys_matrix, dist_matrix):
	differential_matrix = dist_matrix/phys_matrix
# 	print(differential_matrix)
	stress_matrix = np.nansum(differential_matrix, axis=1)
	
	return stress_matrix
		

def generateReport(stress_matrix, fileName):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.39.224', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	node_index = getNodeIndex()
	
	fileName = fileName + ".csv"
	
	sqlcursor.execute("SELECT atlas.resource_name, node_index_day.index, atlas.lat, atlas.long FROM atlas INNER JOIN node_index_day ON atlas.resource_name = node_index_day.resource_name ORDER BY node_index_day.index;")
	
	results = sqlcursor.fetchall()
	
	with open(fileName, 'w') as outputFile:
		line = "resource_name,lat,long,stress\n"
		outputFile.write(line)
		for result in results:
			resource_name = result[0]
			lat = result[2]
			long = result[3]
			stress = stress_matrix[result[1]]
			line = resource_name + "," + str(lat) + "," + str(long) + "," + str(stress) + '\n'
			outputFile.write(line)
	
	
	


if __name__ == "__main__":

	phys_matrix = generatePhysicalMatrix()
	
	stress_matrix_list = []

	dist_matrix_list = generateDistMatrixList()
	
	print(len(dist_matrix_list))
	
	for dist_matrix in dist_matrix_list:
		stress_matrix_list.append(calculateStressMatrix(phys_matrix, dist_matrix))
	
	
	print(len(stress_matrix_list))
		
	stress_matrix = np.zeros(stress_matrix_list[0].shape) 
	
	for sm in stress_matrix_list:
		stress_matrix = stress_matrix + sm
		
	
	dist_matrix = dist_matrix / len(dist_matrix_list)
		
	
	stress_matrix = stress_matrix / len(dist_matrix_list)
	
	generateReport(stress_matrix, 'Annual')


	
	
	
	
	
	
	










