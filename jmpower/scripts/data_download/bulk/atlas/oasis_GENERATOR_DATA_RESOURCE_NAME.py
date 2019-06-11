import psycopg2
import requests
import datetime
import time
import multiprocessing
import random
from io import BytesIO, StringIO
import zipfile
import re


			
def importCAISOData(file):

	pgsqlconnection = psycopg2.connect(database='caiso', user='caiso_bot', password='caiso17bot', host = '172.31.14.212', port='5432')
	sqlcursor = pgsqlconnection.cursor()
	
	f = open(file, 'r')
	
	
	
	sqlcursor.copy_from(f, 'atlas_gen_resource_name', columns=('gen_id', 'name', 'resource_name'), sep=',') 
	
	pgsqlconnection.commit()
	pgsqlconnection.close()

	f.close()
	


	
if __name__ == "__main__":
	

	file = "../../../../data/atlas_gen_data.csv"
	
	importCAISOData(file)

		