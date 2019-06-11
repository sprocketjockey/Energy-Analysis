import psycopg2



#def generateURL():

#Base URL http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime=20120101T07:00-0000&enddatetime=20120101T08:00-0000&version=3&market_run_id=RTM&grp_type=ALL



	
def configureDatabase():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("DROP TABLE IF EXISTS atlas_map;")
	
	pgsqlconnection.commit()

	sqlcursor.execute("CREATE TABLE atlas_map (resource_name text, latitude_known double precision, longitude_known double precision, latitude_estimate double precision, longitude_estimate double precision, nn_1 text, nn_2 text, nn_3 text, fn_1 text, fn_2 text, fn_3 text);")


	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	
if __name__ == "__main__":
	
 	configureDatabase()
	
	

		