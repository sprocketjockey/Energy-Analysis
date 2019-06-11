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
	
	for i in range(populationNumbers):
		
		matrix = generateMatrix(timeIntervals)
		randomizeInputMatrix(matrix, battery)
		simulate(matrix, battery)
		
		spawn_queue.put([matrix[6,-1], 0 ,matrix.copy()])
		
	

def createNextGeneration(spawns, populationNumber):
	workerBattery = Battery()
	mutate_rate = 0.01
	mutate_frequency = 0.5
	
	for i in range(0, populationNumber):
		idx_a = random.randint(0,(len(spawns)-1))
		idx_b = random.randint(0,(len(spawns)-1))
		parent_a = spawns[idx_a][2]
		parent_b = spawns[idx_b][2]
		new_spawn = mate(parent_a, parent_b)
		mu = random.random()
		if(mu < mutate_frequency):
			mutate(new_spawn, mutate_rate)
		simulate(new_spawn, battery)
		spawn_queue.put([new_spawn[6,-1], 0 , new_spawn.copy()])

		
def mutate(new_spawn, mutate_rate):
	
	spawn_size = new_spawn.shape[1]
	mutation_numbers = int(spawn_size * mutate_rate)
	
	action_indexes = np.random.randint(spawn_size, size=mutation_numbers)

	
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
		
		
		# print("O: " + str(old) + " N: " + str(new))

	
	
	
		
def mate(parent_a, parent_b):
	matrix = generateMatrix(parent_a.shape[1])
	
	intervalSize = 1
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
			
def rankSelection(spawns, sample_size):
	selection = spawns[-sample_size:]
	
	return selection
	
def binSelection(spawns, preservation_rate):
	scale_factor = preservation_rate/12
	bin_size = len(spawns)/4
	
	bin_break = [0, bin_size, bin_size * 2, bin_size * 3]
	
	selection_qty = np.array([1,2,3,6]) * scale_factor
	total_selected = np.sum(selection_qty)

	selected = []
	
	for i in range(4):
		for j in range(int(selection_qty[i])):
			idx = random.randint(int(bin_break[i]), int(bin_break[i] + bin_size))
			if (idx < len(spawns)):
				selected.append(spawns[idx])
			else:
				selected.append(spawns[idx - 1])
	
	return selected
	


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

	
def randomizeInputMatrix(matrix, battery):
	
	matrix_size = matrix.shape[1]

	for i in range(matrix.shape[1]):
		control = random.randint(-1,1)
		if control == 1:
			matrix[0, i] = 1
		elif control == 0:
			matrix[0, i] = 0
			matrix[1, i] = 0
		elif control == -1:
			matrix[1, i] = 1
		matrix[2, i] = battery.market_price_list[i + 1]


if __name__ == "__main__":
	battery = Battery()
	timeIntervals = battery.getIntervals()

	totalWorkers = mp.cpu_count()
	subPopulationNumbers = 48
	generationSize = subPopulationNumbers * totalWorkers
	preservation_rate = int(generationSize/4)
	totalGenerations = 1000
	
	
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
	
# 	for i in range(0, preservation_rate):
#  		selected_spawns.append(fitnessSelection(spawns))
	
 	

#  	selected_spawns = binSelection(spawns, preservation_rate)

	selected_spawns = rankSelection(spawns, preservation_rate)
		

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
		
# 		for j in range(0, preservation_rate):
# 			selected_spawns.append(fitnessSelection(spawns))


# 		selected_spawns = binSelection(spawns, preservation_rate)
		selected_spawns = rankSelection(spawns, preservation_rate)
		
		convergence = 0
			
		for selected in selected_spawns:
			print("Raw: " + str(selected[0]) + " Normalized: " + str(selected[1]))
			if selected[1] == 1.0:
				convergence = convergence + 1
			
		if convergence == preservation_rate:
			print("Convergence achieved after " + str(gen) + " generations.")
			break
		
		print("Finalizing Generation: " + str(gen))
		
		print()
	
	
	best = spawns[-1]
	
	best = np.transpose(best[2])
	
	np.savetxt('results.csv', best, delimiter =',')
	
	battery.systemStats()
	
	

