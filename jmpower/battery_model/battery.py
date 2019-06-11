import random
import numpy as np
import scipy.stats
from numba import jit


class Battery:

	def __init__(self):
		
		# Physical Battery Properties
		
		self.battery_power = 50e3  		# Power Watts
		self.inverter_power = 250e3 	# Power Watts
		self.battery_capacity = 720e6 	# Energy in Joules
		self.energy_efficency = 0.88
		
		# Inverter Properties
		
		self.inverter_power = 250e3 # Power Watts
		
		# Cost
		
		self.inverter_cost = 52500 	# Cost in $
		self.inverter_qty = 1
		self.battery_cost = 44500 	# Cost in $
		self.battery_qty = 8
		
		# Energy Price Data
		self.market_price_list = self.loadMarketData()
		
		# Warranty Limits
		
		self.warranty_time = 10 			# Time in years
		self.expected_life_time = 15		# Time in years
		self.warranty_cycle_count = 5000/(10)	# Battery Cycle Count
		
		# Time
		
		self.interval_length_sec = 300 	# Delta T between intervals
		self.interval_count = 0
		
		# System 
		self.system_energy_capacity = self.battery_capacity * self.battery_qty
		self.system_power_capacity = min((self.inverter_power * self.inverter_qty), (self.battery_power * self.battery_qty))
		self.system_capital_cost = self.inverter_qty * self.inverter_cost + self.battery_qty * self.battery_cost
		
		# State
		self.system_charge = 0				# Energy in Joules
		self.charge_cycle_count = 0			# Charge/Discharge Count
		self.revenue_total = 0				# Total financial sales

		
		
		# Stats
		#self.beta = self.calculateBeta()

		self.reset()
		
	def state(self):
		state = (self.system_charge, self.charge_cycle_count, self.revenue_total, self.market_price)
		return state
	
	def systemStats(self):
		print("System Power: " + str(self.system_power_capacity) + " W")
		print("System Energy: " + str(self.system_energy_capacity) + " J")
		print("Annual System Cost: " + str(self.system_capital_cost/self.warranty_time))
	
	
	def step(self, action):
		self.interval_count = self.interval_count + 1
		
		revenue = 0
		
		if (self.interval_count < len(self.market_price_list)):
			self.market_price = self.market_price_list[self.interval_count]
			charge = action[0]
			discharge = action[1]
			bid = action[2]

			if (charge == 1 and discharge == 0 and bid >= self.market_price ):
				revenue = self.calculateSystemChange(1, bid)
			if (charge == 0 and discharge == 1 and bid <= self.market_price):
				revenue = self.calculateSystemChange(-1, bid)
		
			self.revenue_total = self.revenue_total + revenue
		
		return revenue
	
	def generateRandomAction(self):
		action = []
		
		action.append(random.choice([True, False]))
		action.append(random.choice([True, False]))
		action.append(random.randrange(0,2000))
		
		return tuple(action)
		
	
	def calculateSystemChange(self, charge, price):
			energy_change = charge * self.system_power_capacity * self.interval_length_sec
			new_system_charge = self.system_charge + energy_change
			new_cycle_count = self.charge_cycle_count + abs(energy_change)/(2 * self.system_energy_capacity)
			
			revenue = 0
			
			if (new_system_charge >= 0 and new_system_charge <= self.system_energy_capacity):
				if (new_cycle_count <= self.warranty_cycle_count):
					revenue = energy_change * 2.778e-10 * - price
					self.system_charge = new_system_charge
					self.charge_cycle_count = new_cycle_count
			return revenue
		
	def loadMarketData(self):
	
		priceData = []
	
		with open('WHITEWTR.csv') as dataFile:
			for line in dataFile:
				if "resource_name" not in line:
					split_line = line.split(",")
					price = split_line[4]
					
					priceData.append(float(price))
		
	
		return priceData
	
	def getIntervals(self):
		return len(self.market_price_list) -1
	
	def calculateBeta(self):
		return scipy.stats.beta.fit(self.market_price_list)

	def reset(self):
		self.system_charge = 0
		self.charge_cycle_count = 0	
		self.revenue_total = 0
		self.interval_count = 0
		self.market_price = self.market_price_list[0]
		

		
		
		