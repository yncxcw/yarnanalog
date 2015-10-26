from times import Time

class ReduceAttempt:
	state=""
	reduceattempt = {}

	def __init__(self):
		self.reduceattempt = {}
		return
	
	def process_taskAttemptEvent(self,eventType,eventValue):
		if eventType == "REDUCE_ATTEMPT_STARTED":
			self.reduceattempt["attemptId"] = eventValue["attemptId"]
			self.reduceattempt["startTime"]=eventValue["startTime"]
			self.reduceattempt["containerId"] = eventValue["containerId"]
			self.reduceattempt["locality"] = eventValue["locality"]
		elif eventType == "REDUCE_ATTEMPT_FINISHED":
                        self.state = "SUCCEEDED"
			self.reduceattempt["hostname"]         = eventValue["hostname"][1:]	
			self.reduceattempt["finishTime"]       = eventValue["finishTime"]
			self.reduceattempt["taskStatus"]       = eventValue["taskStatus"]
			self.reduceattempt["shuffleFinishTime"]= eventValue["shuffleFinishTime"]
		else:
			self.state = "FAILED"

	def get_finishTime(self):
		return Time(self.reduceattempt["finishTime"])
	def get_shuffleFinishTime(self):
		return Time(self.reduceattempt["shuffleFinishTime"])
	def get_hostname(self):
		return self.reduceattempt["hostname"]
	def get_attemptid(self):
		return self.reduceattempt["attemptId"]

	def get_startTime(self):
		return Time(self.reduceattempt["startTime"])

	def get_locality(self):
		return self.reduceattempt["locality"]
	
	def get_taskStatus(self):
		return self.state


class Reduce:
	reducetask = {}
	reduceattempts = []
	state=""

	def __init__(self):
		self.reducetask = {}
		self.reduceattempts = []
		return
	def process_taskEvent(self,eventType,eventValue):
		if eventType == "TASK_STARTED":
			self.reducetask["taskid"] = eventValue["taskid"]
			self.reducetask["startTime"] = eventValue["startTime"]
		elif eventType == "TASK_FINISHED":
                        self.state = "SUCCEED"
			self.reducetask["finishTime"]=eventValue["finishTime"]
		elif eventType == "TASK_FAILED" or eventType == "TASK_KILLED":
			self.maptask["finishTime"] = eventValue["finishTime"] 
                        self.state = "FAILED" 
		elif eventType.startswith("REDUCE_ATTEMPT"):
			if eventType == "REDUCE_ATTEMPT_STARTED":
				nreduceattempt = ReduceAttempt()
				nreduceattempt.process_taskAttemptEvent(eventType,eventValue)
				self.reduceattempts.append(nreduceattempt)
			else:
				self.get_attempt(eventValue["attemptId"]).process_taskAttemptEvent(eventType,eventValue)

	def get_attempt(self,attemptid):
		for attempt in self.reduceattempts:
			if attempt.get_attemptid()==attemptid:
				return attempt
		return
				
	def get_taskid(self):
		return self.reducetask["taskid"]

	def get_attmpTimes(self):
		return len(self.reduceattempts)

	def get_startTime(self):
		attempt = self.get_successAttempt()
		time = attempt.get_startTime()
		return time;

	def get_finishTime(self):
		attempt = self.get_successAttempt()
		time = attempt.get_finishTime()
		return time

	def get_shuffleFinishTime(self):
		attempt = self.get_successAttempt()
		time = attempt.get_shuffleFinishTime()
		return time


	def get_spendTime(self):
		if self.get_finishTime().__strtoint__() < 0:
			return 0
		elif self.get_startTime().__strtoint__() < 0:
			return 0
		else:
			return self.get_finishTime() - self.get_startTime()
	
	def get_successAttempt(self):
		for attempt in self.reduceattempts:
			if attempt.get_taskStatus() == "SUCCEEDED":
				return attempt
 		
