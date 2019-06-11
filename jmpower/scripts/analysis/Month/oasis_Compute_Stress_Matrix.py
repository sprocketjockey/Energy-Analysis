import psycopg2
import numpy as np
import matplotlib.pyplot as plt


def generatePhysicalMatrix():

	node_index = getNodeIndex()
	
	dist_matrix = np.empty([len(node_index),len(node_index)])
	
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("SELECT resource_name, pair_name, distance_known FROM physical_distance WHERE resource_name IN (SELECT resource_name FROM node_index ORDER BY index) AND pair_name IN (SELECT resource_name FROM node_index ORDER BY index);")
	
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
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT resource_name, index FROM node_index;")
	results = sqlcursor.fetchall()
	
	node_index = {}
	
	for result in results:
		node_index[result[0]] = result[1]
	
	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	return node_index
	
	

def generateEnergyMatrix(index):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("SELECT distance_matrix FROM dtw_dist_month WHERE month = %s;",(index,))
	
	results = sqlcursor.fetchall()
	
	dist_matrix = np.array(results[0][0])

	return dist_matrix

def calculateStressMatrix(phys_matrix, eng_matrix):
	differential_matrix = eng_matrix/phys_matrix
	print(differential_matrix)
	stress_matrix = np.nansum(differential_matrix, axis=1)
	
	return stress_matrix
		

def generateReport(stress_matrix, month):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	node_index = getNodeIndex()
	
	fileName = str(month) + ".csv"
	
	
		
	
	sqlcursor.execute("SELECT atlas.resource_name, node_index.index, atlas.lat, atlas.long FROM atlas INNER JOIN node_index ON atlas.resource_name = node_index.resource_name ORDER BY node_index.index;")
	
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

	for i in range(1,13):
		print("Calculating: " + str(i))
		
		eng_matrix = generateEnergyMatrix(i)
	
		stress_matrix_list.append(calculateStressMatrix(phys_matrix, eng_matrix))
	
		
		
	stress_matrix = np.zeros(stress_matrix_list[1].shape) 
	
	for sm in stress_matrix_list:
		stress_matrix = stress_matrix + sm
		
	
	stress_matrix = stress_matrix / 12
	
	generateReport(stress_matrix, 13)

	
	
	
	
	
	
	










