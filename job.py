from times import Time
from map import Map
from reduce import Reduce


class Job:

	job = {}
	maps= []
	reduces = []
	
	def __init__(self):
		self.maps = []
		self.reduces = []
		return

	def process_jobEvent(self,eventType,eventValue):
		if eventType == "JOB_SUBMITTED":
			self.job["jobid"]    = eventValue["jobid"]
			self.job["userName"] = eventValue["userName"]
			self.job["jobName"]  = eventValue["jobName"]
			self.job["submitTime"] = eventValue["submitTime"]
		elif eventType == "JOB_INITED":
			self.job["launchTime"] = eventValue["launchTime"]
			self.job["totalMaps"]  = eventValue["totalMaps"]
			self.job["toalReduces"]= eventValue["totalReduces"]
		elif eventType == "JOB_FINISHED":
			self.job["finishTime"]   = eventValue["finishTime"]
			self.job["failedMaps"]   = eventValue["failedMaps"]
			self.job["failedReduces"]= eventValue["failedReduces"]

	def process_tasksEvent(self,eventType,eventValue):
		if eventType.startswith("TASK"):
			if eventValue["taskType"] == "MAP":
				if eventType == "TASK_STARTED":
					nmap = Map()
					nmap.process_taskEvent(eventType,eventValue)
					self.maps.append(nmap)
				else:
					self.get_map(eventValue["taskid"]).process_taskEvent(eventType,eventValue)
			elif eventValue["taskType"] == "REDUCE":
				if eventType == "TASK_STARTED":
					nreduce = Reduce()
					nreduce.process_taskEvent(eventType,eventValue)
					self.reduces.append(nreduce)
				else:
					self.get_reduce(eventValue["taskid"]).process_taskEvent(eventType,eventValue)
		elif eventType.startswith("MAP_ATTEMPT"):
			self.get_map(eventValue["taskid"]).process_taskEvent(eventType,eventValue)
		elif eventType.startswith("REDUCE_ATTEMPT"):
			self.get_reduce(eventValue["taskid"]).process_taskEvent(eventType,eventValue)

	def get_map(self,mapid):
		for maptask in self.maps:
			if maptask.get_taskid() == mapid:
				return maptask
		return

	def get_reduce(self,reduceid):
		for reducetask in self.reduces:
			if reducetask.get_taskid() == reduceid:
				return reducetask
		return 		
	def get_numMaps(self):
		return self.job["totalMaps"]

	def get_numReduces(self):
		return self.job["totalReduces"] 

	def get_failedMaps(self):
		return self.job["failedMaps"]

	def get_failedReduces(self):
		return self.job["failedReduces"]

	def get_jobName(self):
		return self.job["jobName"]

	def get_jobID(self):
		return self.job["jobid"]

	def get_userName(self):
		return self.job["userName"]

	def get_submitTime(self):
		time = Time(self.job["submitTime"])
		return time

	def get_launchTime(self):
		time = Time(self.job["launchTime"])
		return time
	
	def get_finishTime(self):
		time = Time(self.job["finishTime"]) 
		return time
	def get_runTime(self):
		if self.get_finishTime().__strtoint__() <=0:
			return 0
		elif self.get_launchTime().__strtoint__() <=0:
			return 0
		else:
			return self.get_finishTime()-self.get_launchTime()
		
	def get_maps(self):
	    tasks = []
            for task in self.maps:
		if task.state == "SUCCEED":
 		    tasks.append(task)            
	    return tasks

	def get_reduces(self):
	    tasks = []
            for task in self.reduces:
		if task.state == "SUCCEED":
 		    tasks.append(task)            
	    return tasks

