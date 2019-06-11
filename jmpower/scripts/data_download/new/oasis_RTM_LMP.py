import psycopg2
import requests
import datetime
import time
import multiprocessing
import random
from io import BytesIO, StringIO
import zipfile


#def generateURL():

#Base URL http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime=20120101T07:00-0000&enddatetime=20120101T08:00-0000&version=3&market_run_id=RTM&grp_type=ALL


def generateOASISRequestURL():

	urlList = multiprocessing.Queue()	
	urlStart = "http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime="
	urlMiddle = "-0000&enddatetime="
	urlEnd = "-0000&version=3&market_run_id=RTM&grp_type=ALL"
	
	initTime = datetime.datetime(2015, 1, 4, 18, 00, 00)
	finalTime = datetime.datetime(2015, 1, 4, 19, 1, 00) 
	startTime = initTime
	hourCounter = 0
	
	while startTime < (finalTime - datetime.timedelta(hours=1)):
		startTime = initTime + datetime.timedelta(hours=hourCounter)
		endTime = startTime + datetime.timedelta(hours=1)
		
		fullURL = urlStart + startTime.strftime("%Y%m%dT%H:%M") + urlMiddle + endTime.strftime("%Y%m%dT%H:%M") + urlEnd
		
		urlList.put(fullURL)
		hourCounter += 1

	return urlList
	
def caisoBotWorker(mgrDict):
	
	run = True
	while run:	
		if urlRequests.empty():
			run = False
		else:
			item = 'empty'
			retry = 0
			downloadCAISOData(item, mgrDict, retry)
	

			
def downloadCAISOData(url, mgrDict, retry):
	try:	
		if (time.time() > mgrDict['lastConnection'] + 12):
			mgrDict['lastConnection'] = time.time()
			if url == 'empty':
				url = urlRequests.get()
			print(url)
			print(datetime.datetime.now())			
			print("Retry: " + str(retry))
			print()
			r = requests.get(url)
			
			report = {}
			
			with zipfile.ZipFile(BytesIO(r.content)) as resultZip:
				for rZip in resultZip.namelist():
					xmlFile = BytesIO(resultZip.read(rZip))
					processCAISOXMLFile(xmlFile, report)
		
			storePriceData(report)
			
		else:
			n = random.random()
			time.sleep(n*12)

			
	except zipfile.BadZipfile:
		retryDownload(url, mgrDict, retry)
		
	except requests.exceptions.ConnectionError:
		retryDownload(url, mgrDict, retry)
		

def retryDownload(url, mgrDict, retry):
	if retry < 5:
		retry += 1
		time.sleep(60)
		downloadCAISOData(url, mgrDict, retry)
	else:
		f = open("Failed Download.txt", 'a')
		f.write(url)
		f.close()
		


def processCAISOXMLFile(xmlByteFile, report):
	
	xmlBytes = xmlByteFile.getvalue()
	xmlFile = xmlBytes.decode('UTF-8')
	
	xmlStringBuffer = StringIO(xmlFile)
	
	dataItem = {}
	
	for line in xmlStringBuffer:
		if "DATA_ITEM" in line:
			dataItem['data_item'] = extractValue(line)
		elif "RESOURCE_NAME" in line:
			dataItem['resource_name'] = extractValue(line)
		elif "OPR_DATE" in line:
			dataItem['date'] = extractValue(line)
		elif "INTERVAL_NUM" in line:
			dataItem['interval'] = extractValue(line)
		elif "INTERVAL_START" in line:
			dataItem['interval_start'] = extractValue(line)
		elif "INTERVAL_END" in line:
			dataItem['interval_end'] = extractValue(line)
		elif "VALUE" in line:
			dataItem['value'] = extractValue(line)
			storeReportValue(report, dataItem)
			dataItem.clear()
	
	xmlStringBuffer.close()
	xmlByteFile.close()

def storeReportValue(report, dataItem):

	if dataItem['resource_name'] not in report:
		report[dataItem['resource_name']] = dict()

	
	if dataItem['interval'] not in report[dataItem['resource_name']]:
		report[dataItem['resource_name']][dataItem['interval']] = dict()
		
	
	dataDict = report[dataItem['resource_name']][dataItem['interval']]

	if 'resource_name' in dataDict:
		dataDict[dataItem['data_item']] = dataItem['value']
	else:	
		dataDict['resource_name'] = dataItem['resource_name']
		dataDict['date'] = dataItem['date']
		dataDict['interval'] = dataItem['interval']
		dataDict['interval_start'] = dataItem['interval_start']
		dataDict['interval_end'] = dataItem['interval_end']
		dataDict[dataItem['data_item']] = dataItem['value']
	
	
def extractValue(line):
	parsedLine = line.replace('<',';')
	parsedLine = parsedLine.replace('>',';')
	splitLine= parsedLine.split(';')
	value = splitLine[2]
	return value
			

def storePriceData(report):
	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')

	sqlcursor = pgsqlconnection.cursor()
	
	reportFile = StringIO()
	
	for resourceID in report:
		for interval in report[resourceID]:
			dataItem = report[resourceID][interval]
			reportLine = dataItem['resource_name'] + ',' + dataItem['date'] + ',' + dataItem['interval_start'] + ',' + dataItem['interval_end'] + ',' + dataItem['interval'] + ',' + dataItem['LMP_PRC'] + ',' + dataItem['LMP_ENE_PRC'] + ',' + dataItem['LMP_CONG_PRC'] + ',' + dataItem['LMP_LOSS_PRC'] + ',' + dataItem['LMP_GHG_PRC'] + '\n'
			reportFile.write(reportLine)

	reportFile.seek(0)

	sqlcursor.copy_from(reportFile, 'lmp_rtm', columns=('resource_name', 'date', 'interval_start', 'interval_end', 'interval', 'lmp_prc', 'lmp_ene_prc', 'lmp_cong_prc', 'lmp_loss_prc', 'lmp_ghg_prc'), sep=',')
	
	pgsqlconnection.commit()
	pgsqlconnection.close()
	reportFile.close()


	
if __name__ == "__main__":
	

	urlRequests = generateOASISRequestURL()
	
	print("Requests: " + str(urlRequests.qsize()))

	workerManager = multiprocessing.Manager()
	
	mgrDict = workerManager.dict()
	mgrDict['lastConnection'] = time.time()
	mgrDict['cleanDatabase'] = False
	
	workers = []
	
	for i in range(1):
		p = multiprocessing.Process(target = caisoBotWorker, args=(mgrDict,))
		workers.append(p)
		p.start()
	
	for p in workers:
		p.join()
	
	urlRequests.close()
	

		
