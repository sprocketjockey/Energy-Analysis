import psycopg2
import numpy as np


def populateMatrixTable():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.36.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT resource_name, index FROM node_index_month;")
	results = sqlcursor.fetchall()
	
	node_index = {}
	
	for result in results:
		node_index[result[0]] = result[1]
	
	print(len(node_index))
	
	dist_matrix = np.empty([len(node_index),len(node_index)])
	
	sqlcursor.execute("SELECT resource_name, pair_name, distance FROM dtw_dist_12")
	
	results = sqlcursor.fetchall()
	
	month = 12
	print(month)
	
	for result in results:
		x = node_index[result[0]]
		y = node_index[result[1]]
		dist = result[2]
		
		dist_matrix[x][y] = dist
	
	print(dist_matrix)
	
	
	dist_matrix_py = dist_matrix.tolist()
	
	sqlcursor.execute("INSERT INTO dtw_dist_month (month, distance_matrix) VALUES (%s, %s);", (month, list(dist_matrix_py)))
	
	pgsqlconnection.commit()
	pgsqlconnection.close()

def createMonthMatrixTable():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.36.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("DROP TABLE IF EXISTS dtw_dist_month;") 

	sqlcursor.execute("CREATE TABLE dtw_dist_month (month integer, distance_matrix double precision [][]);")

	pgsqlconnection.commit()
	pgsqlconnection.close()



if __name__ == "__main__":
	
#	createMonthMatrixTable()
	
	populateMatrixTable()
	


	
	
	
	










