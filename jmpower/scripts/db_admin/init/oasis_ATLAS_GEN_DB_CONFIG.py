import psycopg2



#def generateURL():

#Base URL http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime=20120101T07:00-0000&enddatetime=20120101T08:00-0000&version=3&market_run_id=RTM&grp_type=ALL



	
def configureDatabase():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("DROP TABLE IF EXISTS atlas_gen;")
	
	pgsqlconnection.commit()

	sqlcursor.execute("CREATE TABLE atlas_gen (resource_name text, gen_id text, resource_id text, udc_id text, cec_plant_id text, trade_hub text, balance_area_authority text, name text, resource_type text, energy_type text, fuel_type text, power_capacity real, latitude double precision, longitude double precision, online_date date, offline_date date);")


	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	
if __name__ == "__main__":
	
 	configureDatabase()
	
	

		