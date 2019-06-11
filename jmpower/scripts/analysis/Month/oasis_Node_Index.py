import psycopg2
import datetime


def populateIndex(startDate, endDate):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.36.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT resource_name FROM atlas WHERE online <= %s::date AND offline >= %s::date AND lat BETWEEN 32 AND 35 AND long BETWEEN -119 AND -116 ORDER BY resource_name;", (startDate, endDate))
	
	idx = 0
	
	for result in sqlcursor.fetchall():
		sqlcursor.execute("INSERT INTO node_index_month (resource_name, index) VALUES (%s, %s);", (result[0], idx))
		idx = idx + 1
	
	pgsqlconnection.commit()
	pgsqlconnection.close()

def createIndexTable():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.36.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("DROP TABLE IF EXISTS node_index_month;") 

	sqlcursor.execute("CREATE TABLE node_index_month (resource_name text, index integer);")

	pgsqlconnection.commit()
	pgsqlconnection.close()



if __name__ == "__main__":
	
	startDate = datetime.date(2017, 1, 1)
	endDate = datetime.date(2017, 3, 31)
	
	createIndexTable()
	
	populateIndex(startDate, endDate)
	


	
	
	
	










