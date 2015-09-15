from times import Time
from counter import Counter
class MapAttempt:
        state=""
	mapattempt = {}

	def __init__(self):
		self.mapattempt = {}
		return

	def process_taskAttemptEvent(self,eventType,eventValue):
		if eventType == "MAP_ATTEMPT_STARTED":
			self.mapattempt["attemptId"]=eventValue["attemptId"]
			self.mapattempt["startTime"]=eventValue["startTime"]
			self.mapattempt["containerId"] = eventValue["containerId"]
			self.mapattempt["locality"] = eventValue["locality"]["string"][0]
		elif eventType == "MAP_ATTEMPT_FINISHED":
                        self.state = "SUCCEED" 
			self.mapattempt["finishTime"] = eventValue["finishTime"]
			self.mapattempt["hostname"]   = eventValue["hostname"][1:]
			self.mapattempt["taskStatus"] = eventValue["taskStatus"]
		else:
			self.state = "FAILED"
	 	
	def get_taskStatus(self):
		return self.state

	def get_hostname(self):
		return self.mapattempt["hostname"]

	def get_attempid(self):
		return self.mapattempt["attemptId"]

	def get_startTime(self):
		time = Time(self.mapattempt["startTime"])
		return time

	def get_locality(self):
		return self.mapattempt["locality"]

	def get_containerId(self):
		return self.mapattempt["containerId"]

	def get_finishTime(self):
		time = Time(self.mapattempt["finishTime"])
		return time
	
	def get_spendTime(self):
		if self.get_finishTime().__strtoint__() <= 0:
			return 0
		elif self.get_startTime().__strtoint__() <= 0:
			return 0
		else:
			return self.get_finishTime()-self.get_startTime()
    

		
class Map:
        state   = ""
	maptask = {}
	mapattempts = [] 

	def __init__(self):
		self.maptask = {}
		self.mapattempts = []
		self.counter = Counter()
		self.register_counter()
		return

	def register_counter(self):
		self.counter.register_property("FILE_BYTES_READ")
		self.counter.register_property("FILE_BYTES_WRITTEN")
		self.counter.register_property("HDFS_BYTES_READ")
		self.counter.register_property("HDFS_BYTES_WRITTEN")
		self.counter.register_property("MAP_INPUT_RECORDS")
		self.counter.register_property("MAP_OUTPUT_RECORDS")
		self.counter.register_property("SPLIT_RAW_BYTES")
		self.counter.register_property("COMBINE_INPUT_RECORDS")
		self.counter.register_property("COMBINE_OUTPUT_RECORDS")
		self.counter.register_property("SPILLED_RECORDS")
		self.counter.register_property("CPU_MILLISECONDS")
		self.counter.register_property("PHYSICAL_MEMORY_BYTES")
		self.counter.register_property("VIRTUAL_MEMORY_BYTES")
		self.counter.register_property("COMMITTED_HEAP_BYTES")
		self.counter.register_property("GC_TIME_MILLIS")
		


	def get_successAttempt(self):
		for attempt in self.mapattempts:
			if attempt.get_taskStatus() == "SUCCEED":
				return attempt
	
	def get_counterValue(self,key):
		return self.counter.get_value(key)

	def process_counterValue(self,counterList):
		for counterValue in counterList:
			self.counter.add_property(counterValue["name"],counterValue["value"]
)
			
					
	def process_taskEvent(self,eventType,eventValue):
		if eventType == "TASK_STARTED":
			self.maptask["taskid"] = eventValue["taskid"]
			self.maptask["startTime"] = eventValue["startTime"]
			self.maptask["splitLocations"] = eventValue["splitLocations"]
		elif eventType == "TASK_FINISHED":
			self.maptask["finishTime"] = eventValue["finishTime"]
                        self.state = "SUCCEED"
			for groupList in eventValue["counters"]["groups"]:
				self.process_counterValue(groupList["counts"])
                elif eventType == "TASK_FAILED" or eventType == "TASK_KILLED":
			self.maptask["finishTime"] = eventValue["finishTime"] 
                        self.state = "FAILED"
		elif eventType.startswith("MAP_ATTEMPT"):
			if eventType == "MAP_ATTEMPT_STARTED":
				nmapattempt = MapAttempt()
				nmapattempt.process_taskAttemptEvent(eventType,eventValue)
				self.mapattempts.append(nmapattempt)
			else:
				self.get_attempt(eventValue["attemptId"]).process_taskAttemptEvent(eventType,eventValue)

	def get_attempt(self,attemptid):	 
		for attempt in self.mapattempts:
			if attempt.get_attempid() == attemptid:
				return attempt
		return
	
	def get_splitLocations(self):
		return self.maptask["splitLocations"]

	def get_taskid(self):
		return self.maptask["taskid"]

	def get_attmpTimes(self):
		return len(self.mapattempts)


	def get_startTime(self):
		attempt = self.get_successAttempt()
		time = attempt.get_startTime()
		return time;

	def get_finishTime(self):
		attempt = self.get_successAttempt()
		time = attempt.get_finishTime()
		return time

	def get_spendTime(self):
		if self.get_finishTime().__strtoint__() <= 0:
			return 0
		elif self.get_startTime().__strtoint__() <= 0:
			return 0
		else:
			return self.get_finishTime()-self.get_startTime()
