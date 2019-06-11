import numpy as np
from battery import Battery

def loadMarketData():
	
	priceData = []
	
	with open('WHITEWTR.csv') as dataFile:
		for line in dataFile:
			if "resource_name" not in line:
				split_line = line.split(",")
				interval = split_line[2]
				price = split_line[4]
				
				priceData.append([int(interval),float(price)])
			
	return priceData

def convertTo2D(priceData):
	
	matrix = np.zeros((288,365))
	
	dayCount = -1
	
	for item in priceData:
		index = item[0] - 1
		price = item[1]
		
		if index == 0:
			dayCount = dayCount + 1
		
		if (index < 288 and dayCount < 365):
			matrix[index, dayCount] = price
	matrix = np.transpose(matrix)
	return matrix
	
def calculatePriceStats(matrix):
	mean = np.mean(matrix, axis = 0)
	median = np.median(matrix, axis = 0)

	day_mean = np.mean(mean)
	day_median = np.mean(median)
	
	return day_mean, mean, day_median, median

def runSimulator(over, under):
	battery = Battery()
	priceData = loadMarketData()
	
	matrix = convertTo2D(priceData)
	
	day_mean, mean, day_median, median = calculatePriceStats(matrix)
	
	for data in priceData:
		 interval = data[0] - 1
		 bid = 0
		 
		 if (interval < 288):
		 	bid = median[interval]
		 
		 
		 if (bid * (1 + over))  > day_median:
		 	action = (0,1, (bid * (1 + over)))
		 	battery.step(action)
		 elif (bid * under) < day_median:
		 	action = (1,0,(bid * under))
		 	battery.step(action)
		 elif bid == 0:
		 	action = (0,0,bid)
		 	battery.step(action)
		 else:
		 	action = (0,0,bid)
		 	battery.step(action)
	
	revenue = battery.state()[2]
	print("Run Finished: Over bid:" + str((over) * 100) + " Under Bid: " + str(under * 100) + " Total Revenue: " + str(revenue))
	battery.reset()
	return revenue
	


def main():
	max_revenue = 0
	for i in range (20):
		for j in range(20):
			revenue = runSimulator(i/100, j/100)
			if (max_revenue < revenue):
				max_revenue = revenue
			print("Max revenue: " + str(max_revenue))
				

		
		
		
	

if __name__ == "__main__":
	main()