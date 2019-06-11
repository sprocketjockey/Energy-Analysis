import numpy as np
from battery import Battery
import random
import multiprocessing as mp
import time
import queue
from numba import jit
from operator import itemgetter
import math
import scipy.stats


def initPopulationGenerator(populationNumbers, timeIntervals):
	
	workerBattery = Battery()
	beta = workerBattery.beta
	
	for i in range(populationNumbers):
		
		matrix = generateMatrix(timeIntervals)
		randomizeInputMatrix(matrix, beta)
		simulate(matrix, battery)
		
		spawn_queue.put([matrix[6,-1], 0 ,matrix.copy()])
		
	

def createNextGeneration(spawns, populationNumber):
	workerBattery = Battery()
	beta = workerBattery.beta
	mutate_frequency = 0.5
	mutate_rate = 1
	
	for i in range(0, populationNumber):
		idx_a = random.randint(0,(len(spawns)-1))
		idx_b = random.randint(0,(len(spawns)-1))
		parent_a = spawns[idx_a][2]
		parent_b = spawns[idx_b][2]
		new_spawn = mate(parent_a, parent_b)
		mu = random.random()
		if(mu < mutate_frequency):
			mutate(new_spawn, mutate_rate, beta)
		simulate(new_spawn, battery)
		spawn_queue.put([new_spawn[6,-1], 0 , new_spawn.copy()])

		
def mutate(new_spawn, mutate_rate, beta):
	
	spawn_size = new_spawn.shape[1]
	mutation_numbers = int(spawn_size * mutate_rate/100)
	
	action_indexes = np.random.randint(spawn_size, size=mutation_numbers)
	bid_indexes = np.random.randint(spawn_size, size=mutation_numbers)
	bid_amounts = scipy.stats.beta.rvs(beta[0], beta[1], beta[2], beta[3], size = mutation_numbers)
	
	for i in range(mutation_numbers):
		control = random.randint(-1,1)
		action_idx = action_indexes[i] 
		if control == 1:
			new_spawn[0, action_idx] = 1
		elif control == 0:
			new_spawn[0, action_idx] = 0
			new_spawn[1, action_idx] = 0
		elif control == -1:
			new_spawn[1, action_idx] = 1
		
		bid_idx = bid_indexes[i]
		
		old = new_spawn[2, bid_idx]
		new_spawn[2, bid_idx] = bid_amounts[i]
		new = new_spawn[2, bid_idx]
		
		# print("O: " + str(old) + " N: " + str(new))

	
	
	
		
def mate(parent_a, parent_b):
	matrix = generateMatrix(parent_a.shape[1])
	
	intervalSize = 96
	intervals = int(parent_a.shape[1]/intervalSize)

	for i in range(0, intervals):
		selector = random.choice([True, False])
		start = i * intervalSize
		end = start + intervalSize
		
		if selector:	
			#print("A - S:" + str(start) +  " E: " + str(end))
			subMatrix = parent_a[0:3,start:end]
			matrix[0:3, start:end] = subMatrix

		else:
			#print("B - S:" + str(start) +  " E: " + str(end))
			subMatrix = parent_b[0:3,start:end]
			matrix[0:3, start:end] = subMatrix
	return matrix


def fitnessSelection(spawns):
	max = sum(s[1] for s in spawns)
	pick = random.uniform(0,max)
	
	current = 0
	for spawn in spawns:
		current = current + spawn[1]
		if current > pick:
			return spawn



def simulate(matrix, battery):
	battery.reset()
	
	for i in range(matrix.shape[1]):
		action = (matrix[0,i], matrix[1,i], matrix[2,i])
		matrix[3,i] = battery.step(action)
		state = battery.state()
		matrix[4,i] = state[0]
		matrix[5,i] = state[1]
		matrix[6,i] = state[2]
		matrix[7,i] = state[3]
		

def linearizeSpawns(spawns):
	spawn_list = []
	
	for s in spawns:
		for i in s:
			spawn_list.append(i)

	spawns = spawn_list
	
	return spawns
	
	
def normalizeSpawns(spawns):
	min = spawns[0][0]
	max = spawns[0][0]
	
	for s in spawns:
		if min > s[0]:
			min = s[0]
		if max < s[0]:
			max = s[0]
			
	for s in spawns:
		s[1] = (s[0] - min)/(max - min)
	
	spawns = sorted(spawns, key=itemgetter(1))
	return spawns
	

def generateMatrix(intervals):
	matrix = np.zeros([8,intervals])
	
	return matrix

	
def randomizeInputMatrix(matrix, beta):
	
	matrix_size = matrix.shape[1]
	random_prices = scipy.stats.beta.rvs(beta[0], beta[1], beta[2], beta[3], size = matrix_size)

	for i in range(matrix.shape[1]):
		control = random.randint(-1,1)
		if control == 1:
			matrix[0, i] = 1
		elif control == 0:
			matrix[0, i] = 0
			matrix[1, i] = 0
		elif control == -1:
			matrix[1, i] = 1
		matrix[2, i] = random_prices[i]


if __name__ == "__main__":
	battery = Battery()
	timeIntervals = battery.getIntervals()

	totalWorkers = mp.cpu_count()
	subPopulationNumbers = 512
	generationSize = subPopulationNumbers * totalWorkers
	preservation_rate = int(generationSize/4)
	totalGenerations = 100
	
	
	pool = mp.Pool(processes=totalWorkers)
	spawn_queue = mp.Queue()
	
	init_workers = []
	
	print("Creating initial generation.")
	
	for i in range(totalWorkers):
		p = mp.Process(target = initPopulationGenerator, args=(subPopulationNumbers, timeIntervals))
		init_workers.append(p)
		p.start()


	spawns = []
	
	for r in range(generationSize):
		spawns.append(spawn_queue.get())
	
	for p in init_workers:
		p.join()
	
	spawns = normalizeSpawns(spawns)

	for spawn in spawns:
		print("Raw: " + str(spawn[0]) + " Normalized: " + str(spawn[1]))

	print("Selecting:")
	
	selected_spawns = []
	
	for i in range(0, preservation_rate):
		selected_spawns.append(fitnessSelection(spawns))
		

	spawns.clear()
	for selected in selected_spawns:
		print("Raw: " + str(selected[0]) + " Normalized: " + str(selected[1]))

	print("Initial Population Formed")
	print()
	
	
	
	
	for gen in range(totalGenerations):
		print("Creating Generation: " + str(gen))

		workers = []
	
		for i in range(totalWorkers):
			p = mp.Process(target = createNextGeneration, args=(selected_spawns, subPopulationNumbers))
			workers.append(p)
			p.start()

		spawns.clear()
		spawns = []
	
		for r in range(generationSize):
			spawns.append(spawn_queue.get())
	
		for p in workers:
			p.join()

		spawns = normalizeSpawns(spawns)
	
	
		for spawn in spawns:
			print("Raw: " + str(spawn[0]) + " Normalized: " + str(spawn[1]))
	
		print("Selecting:")
		
		selected_spawns.clear()
		
		for j in range(0, preservation_rate):
			selected_spawns.append(fitnessSelection(spawns))
			
		for selected in selected_spawns:
			print("Raw: " + str(selected[0]) + " Normalized: " + str(selected[1]))
		
		print("Finalizing Generation: " + str(gen))
		print()
	
	
	

