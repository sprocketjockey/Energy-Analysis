import psycopg2



#def generateURL():

#Base URL http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime=20120101T07:00-0000&enddatetime=20120101T08:00-0000&version=3&market_run_id=RTM&grp_type=ALL



	
def configureDatabase():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("DROP TABLE IF EXISTS lmp_rtm;")
	
	pgsqlconnection.commit()

	sqlcursor.execute("CREATE TABLE lmp_rtm (resource_name text, date date, interval_start timestamp, interval_end timestamp, interval int, lmp_prc money, lmp_ene_prc money, lmp_cong_prc money, lmp_loss_prc money, lmp_ghg_prc money);")
	sqlcursor.execute("CREATE INDEX idx_resource_name ON lmp_rtm(resource_name);")
	sqlcursor.execute("CREATE INDEX idx_interval_start ON lmp_rtm(interval_start);")
	

	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	
if __name__ == "__main__":
	
 	configureDatabase()
	
	

		