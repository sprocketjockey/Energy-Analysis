import psycopg2
import requests
import datetime
import time
import multiprocessing
import random
from io import BytesIO, StringIO
import zipfile
import re


			
def downloadCAISOData(url):

	print(url)
	print(datetime.datetime.now())			

	r = requests.get(url)
	
	xmlByteFile = BytesIO(r.content)
	
	processCAISOXMLFile(xmlByteFile)



def processCAISOXMLFile(xmlByteFile):
	
	xmlBytes = xmlByteFile.getvalue()
	xmlFile = xmlBytes.decode('UTF-8')
	nodes = xmlFile.split(',{')
	
	report = {}
	
	for node in nodes:

		nodeName = re.search('(?<="n":")\w*', node)
		if nodeName:
			name = nodeName.group(0)
			report['resource_name'] = name
			
		nodeLocation = re.search('(?<="c":)[[].*?[]]', node)
		if nodeLocation:
			location = nodeLocation.group(0)
			location = location.replace('[', '')
			location = location.replace(']', '')
			location = location.split(',')
			latitude = location[0]
			longitude = location[1]
			
			report['lat'] = latitude
			report['long'] = longitude

			
		nodeType = re.search('(?<="p":")\w*', node)
		if nodeType:
			type = nodeType.group(0)
			report['type'] = type
		
# 		print(report)
# 		print()
		storeData(report)
	
	xmlByteFile.close()

def storeData(report):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')
	sqlcursor = pgsqlconnection.cursor()
	
	if 'resource_name' in report:
		sqlcursor.execute('UPDATE atlas SET type=%s, lat=%s, long=%s WHERE resource_name = %s',(report['type'], report['lat'], report['long'], report['resource_name']))

	pgsqlconnection.commit()
	pgsqlconnection.close()

	


	
if __name__ == "__main__":
	

	url = 'http://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1'
	
	downloadCAISOData(url)

		