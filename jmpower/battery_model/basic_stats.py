import numpy as np
import matplotlib.pyplot as plt
from scipy.integrate import simps
import scipy
import scipy.stats
import random

def loadMarketData():
	
	priceData = []
	
	with open('WHITEWTR.csv') as dataFile:
		for line in dataFile:
			if "resource_name" not in line:
				split_line = line.split(",")
				#interval = split_line[2]
				price = split_line[4]
				priceData.append(float(price))
				#priceData.append([int(interval),float(price)])
			
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
	x = np.linspace(0,1,288)
	
	combo = mean - median
	
	day_mean = np.mean(mean)
	day_median = np.mean(median)
	
	day = day_mean - day_median
	
	mean = mean - (day_mean)
	median = median - day_median
	
	area = simps(mean, x)
	
	print(day_median + 5)
	print(area)
	
	

if __name__ == "__main__":
	
	priceData = loadMarketData()
	
	#matrix = convertTo2D(priceData)
	
	#calculatePriceStats(matrix)
	
# 	size = len(priceData)
# 	
# 	x = scipy.arange(size)
	y = np.array(priceData)
# 	
# 	h = plt.hist(y, bins=500)
# 	
# 	dist_names = ['alpha','gamma', 'beta', 'rayleigh', 'norm', 'pareto']
# 	
# 	for dist_name in dist_names:
# 		dist = getattr(scipy.stats, dist_name)
# 		param = dist.fit(y)
# 		pdf_fitted = dist.pdf(x, *param[:-2], loc=param[-2], scale=param[-1]) * size
# 		plt.plot(pdf_fitted, label=dist_name)
# 		plt.xlim(-150,300)
# 	plt.legend(loc='upper right')
# 	plt.show()
	
	mu = np.mean(priceData)
	sigma = np.std(priceData)
	
	
	alpha = scipy.stats.alpha.fit(priceData)
	beta = scipy.stats.beta.fit(priceData)
	gamma = scipy.stats.gamma.fit(priceData)
	lognorm = scipy.stats.lognorm.fit(priceData)
	
	min = np.min(priceData)
	max = np.max(priceData)
	
	print("Mean: " + str(mu))
	print("STD: " + str(sigma))
	print("alpha: " + str(alpha))
	print("beta: " + str(beta))
	print("gamma: " + str(gamma))
	print("lognorm: " + str(lognorm))
	
	
	#random_gen = scipy.stats.alpha.rvs(alpha[0], alpha[1], alpha[2], size=100000)
	random_gen = scipy.stats.beta.rvs(beta[0], beta[1], beta[2], beta[3], size=100000)
	#random_gen = scipy.stats.gamma.rvs(gamma[0], gamma[1], gamma[2], size=100000)


	y1 = np.array(random_gen)
	
	h = plt.hist(y1, bins=100)
	
	plt.show()
	
# 	plot = plt.plot(x,mean)
# 	plot = plt.plot(x, median)
# 	plot = plt.hlines(0, 0,1)
# 	
# 	Sell if mean[i] > day_mean, buy if mean[i] < day_mean 
# 
# 	
# 
# 	plot = plt.plot(x, combo)
# 	plot = plt.hlines(day, 0, 1)
# 	
# 	plot = plt.boxplot(matrix, showfliers=False, showmeans=True)
# 	
# 	plt.show()
	