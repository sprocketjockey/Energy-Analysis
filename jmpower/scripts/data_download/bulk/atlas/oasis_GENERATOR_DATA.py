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
	
	
	
	sqlcursor.copy_from(f, 'atlas_gen', columns=('gen_id', 'resource_id', 'resource_type', 'name', 'power_capacity', 'energy_type', 'fuel_type', 'trade_hub', 'balance_area_authority', 'udc_id'), sep=',') 
	
	pgsqlconnection.commit()
	pgsqlconnection.close()

	f.close()
	


	
if __name__ == "__main__":
	

	file = "../../../../data/ATL_GEN_CAP_LST.csv"
	
	importCAISOData(file)

		