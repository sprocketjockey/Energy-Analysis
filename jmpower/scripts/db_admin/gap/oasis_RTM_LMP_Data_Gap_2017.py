import psycopg2
import datetime

if __name__ == "__main__":
	
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	print("Querying Database")
	
	sqlcursor.execute("SELECT DISTINCT interval_start FROM lmp_rtm WHERE interval_start >= '2017-01-01'::date AND interval_start < '2018-01-01'::date ORDER BY interval_start;")
	
	
	intervals = sqlcursor.fetchall()
	
	print("Processing Result")
	
	firstInterval = intervals[0][0]
	lastInterval = intervals[0][0]
	
	for interval in intervals:
		
		date = interval[0]

		if firstInterval != interval:
			if lastInterval == date:
				print('Duplicate found: ' + date.isoformat())
				f = open('../../../results/duplicates_2017.txt', 'a')
				line = date.isoformat()  + '\n'
				f.write(line)
				f.close()
			if date > lastInterval + datetime.timedelta(minutes=5):
				print('Gap found: S:' + lastInterval.isoformat() + ' E:' + date.isoformat())
				line = 'S:' + lastInterval.isoformat() + ' E:' + date.isoformat() + '\n'
				f = open('../../../results/gap_2017.txt', 'a')
				f.write(line)
				f.close()
				
			
			lastInterval = date 
	
	print(len(intervals))

	pgsqlconnection.commit()
	pgsqlconnection.close()
