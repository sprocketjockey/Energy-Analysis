import psycopg2
import datetime
	
def getIntervals(startDate, endDate):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("SELECT DISTINCT(interval_start) FROM lmp_rtm WHERE date BETWEEN %s AND %s ORDER BY interval_start;", (startDate, endDate))
	
	results = sqlcursor.fetchall()
	
	intervals = []

	for result in results:
		intervals.append(result[0])


	pgsqlconnection.commit()
	pgsqlconnection.close()
	
	return intervals

def populatePriceCache(startDate, endDate, intervals):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')

	sqlcursor = pgsqlconnection.cursor()

	sqlcursor.execute("INSERT INTO price_cache (resource_name, price_array) SELECT resource_name, array_agg(cast(lmp_prc::numeric as double precision)) FROM lmp_rtm WHERE resource_name IN (SELECT resource_name FROM atlas WHERE online <= %s::date AND offline >= %s::date AND lat BETWEEN 32 AND 35 AND long BETWEEN -119 AND -116) AND date BETWEEN %s::date AND %s::date GROUP BY resource_name HAVING count(interval_start) = %s ORDER BY resource_name;", (startDate, endDate, startDate, endDate, intervals))
	
	pgsqlconnection.commit()
	pgsqlconnection.close()

def createCacheTable():
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '10.0.1.1', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	sqlcursor.execute("DROP TABLE IF EXISTS cosign_sim;") 

	sqlcursor.execute("CREATE TABLE cosign_sim (resource_name text, pair_name text, similarity double precision);")
	
	sqlcursor.execute("DROP TABLE IF EXISTS price_cache;")
	
	sqlcursor.execute("CREATE TABLE price_cache (id serial, resource_name text, price_array double precision[]);")
	
	sqlcursor.execute("CREATE INDEX idx_price_cache ON price_cache(id);")

	pgsqlconnection.commit()
	pgsqlconnection.close()



if __name__ == "__main__":
	
	startDate = datetime.date(2017, 6, 15)
	endDate = datetime.date(2017, 6, 21)
	
	print("Creating Table...")
	createCacheTable()
	
	print("Get Intervals...")
	intervalList = getIntervals(startDate, endDate)
	
	intervalCount = int(len(intervalList))
	
	print("Intervals: " + str(intervalCount))
	
	print("Populate price cache...")
	populatePriceCache(startDate, endDate, intervalCount)
	


	
	
	
	










