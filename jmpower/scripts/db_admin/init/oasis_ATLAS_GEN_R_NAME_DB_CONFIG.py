import psycopg2



#def generateURL():

#Base URL http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime=20120101T07:00-0000&enddatetime=20120101T08:00-0000&version=3&market_run_id=RTM&grp_type=ALL



	
def configureDatabase():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("DROP TABLE IF EXISTS atlas_gen_resource_name;")
	
	pgsqlconnection.commit()

	sqlcursor.execute("CREATE TABLE atlas_gen_resource_name (resource_name text, gen_id text, name text);")


	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	
if __name__ == "__main__":
	
 	configureDatabase()
	
	

		