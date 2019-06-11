import psycopg2



#def generateURL():

#Base URL http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime=20120101T07:00-0000&enddatetime=20120101T08:00-0000&version=3&market_run_id=RTM&grp_type=ALL



	
def configureDatabase():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("DROP TABLE IF EXISTS atlas;")
	
	pgsqlconnection.commit()

	sqlcursor.execute("CREATE TABLE atlas (resource_name text, type text, lat double precision, long double precision, online date, offline date);")
	
	print("Populating Nodes into Table")
	
	sqlcursor.execute("INSERT INTO atlas(resource_name) SELECT DISTINCT resource_name FROM lmp_rtm;")
	
	

	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	
if __name__ == "__main__":
	
 	configureDatabase()
	
	

		