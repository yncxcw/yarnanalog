import json
import numpy as np
from job import Job
from event import Event
import draw as draw
import matplotlib.pyplot as pl
from operator import itemgetter

class TraceBuild:
	json_path = ""
  

	def __init__(self,json_path_t):
		self.json_path = json_path_t
		self.job = Job()

	def load(self):
		try:
			self.json_file=open(self.json_path,'r')
		except IOError as error:
			print ("file open error:"+str(error))
			return 0
		return 1
	
	def initial_jobs(self):
		while True:
			json_str=self.json_file.readline()
			if not json_str:
				break
			elif not json_str.startswith("{"):
				continue
			else:
				json_dict = json.loads(json_str)
				event = Event(json_dict)
				event.process_event(self.job)

		return 1


	#def get_jobbyid(self,jobid):
		
    #def get_jobbyindex(self,index):
	def reduce_startTime(self):
		times = []
		reduces = self.job.get_reduces()
		for reducetask in reduces:
			times.append(reducetask.reduceattempts[0].get_startTime().__strtoint__())
		return times

	def reduce_finishTime(self):
		times = []
		reduces = self.job.get_reduces()
		for reducetask in reduces:
			times.append(reducetask.get_finishTime().__strtoint__())
		return times


	def map_startTime(self):
		times = []
		maps = self.job.get_maps()
		for maptask in maps:
			#times.append(maptask.mapattempts[0].get_startTime().__strtoint__())
			times.append(maptask.get_startTime().__strtoint__())
		return times

	def map_finishTime(self):
		times = []
		maps = self.job.get_maps()
		for maptask in maps:
			times.append(maptask.get_finishTime().__strtoint__())
		return times

	def reduce_shufflefinishTime(self):
		times = []
		reduces = self.job.get_reduces()
		for reducetask in reduces:
			times.append(reducetask.get_shuffleFinishTime().__strtoint__())
		return times


	def map_countByKey(self,key):
		countValues = []
		maps = self.job.get_maps()
		for maptask in maps:
			countValues.append(maptask.get_counterValue(key))	
		return countValues		

	def map_time(self):

		times=[]
		maps = self.job.get_maps()
		for maptask in maps:
			times.append(maptask.get_spendTime())
		return times

	def reduce_time(self):
		times=[]
		reduces = self.job.get_reduces()
		for reducetask in reduces:
			times.append(reducetask.get_spendTime())
		return times

	def job_runTinme(self):
		return self.job.get_runTime()

	def get_mapAttemptLocality(self):
		locality = []
		maps = self.job.get_maps()
		for maptask in maps:
			locality.append(maptask.get_successAttempt().get_locality())
		return locality

	def get_mapAttemptHost(self):
		host = []
		for maptask in self.job.get_maps():
			host.append(maptask.get_successAttempt().get_hostname())
		return host

	def get_reduceAttemptHost(self):
		host = []
		for reducetask in self.job.get_reduces():
			host.append(reducetask.get_successAttempt().get_hostname())
		return host

	def get_maps(self):
		return self.job.get_maps()

	def get_reduces(self):
		return self.job.get_reduces()

	def normalrize(self,dataSet):
		normalSet = []
		tSet = []
		for i in range(0,len(dataSet)):
			tSet.append(dataSet[i])

		for i in range(0,len(dataSet)):
			normal = (float)(tSet[i])/(float)(sum(tSet))
			normalSet.append(normal)
		return normalSet

def draw_bar(plt,job_starttime,start_times,finish_times,shuffle_times,host):

	for i in range(0,len(start_times)):
		start_times[i]=start_times[i] - job_starttime

	for i in range(0,len(start_times)):
		finish_times[i]=finish_times[i] - job_starttime
	for i in range(0,len(shuffle_times)):
		shuffle_times[i]=shuffle_times[i] - job_starttime

	print "size jobs",len(start_times)

	L = range(1,len(start_times)+1)
	H = []
	for i in range(0,len(start_times)):
		H.append(finish_times[i]-start_times[i])
	tuple_list = []
	for i in range(0,len(host)):
		tuple_list.append((start_times[i],H[i],host[i]))
	tuple_list = sorted(tuple_list,key=itemgetter(2))
	
	W = 0.8
	B = start_times
        X = []
        Z = []
	shuffle_start=len(start_times)-len(shuffle_times)
	for i in range(0,len(start_times)):
		X.append(2*L[i]-1)
		Z.append(tuple_list[i][2])
		plt.bar(2*L[i]-1,tuple_list[i][1],W,bottom=tuple_list[i][0])

	##for i in range(shuffle_start,len(start_times)):
	##	plt.bar(2*L[i]-1,shuffle_times[i-shuffle_start]-B[i],W,bottom=B[i],color='r')
	print Z
	plt.xticks(X,Z)
	plt.show() 

if __name__ == "__main__":
	
	traceBuilder = TraceBuild("./job.jhist")
	if traceBuilder.load() == 0:
		print "load error"
#        return 
	if traceBuilder.initial_jobs() == 0:
		print "initial job error"
#		return

	print "job run time"
	print traceBuilder.job_runTinme()

	reducetimes = traceBuilder.reduce_time()
	print "size reduce",len(reducetimes)
	print "reduce times"
 
	maptimes = traceBuilder.map_time()
	print "size maps",len(maptimes)
	print "map times"

	num_bins=50
	maptimes.sort()

	for i in range(1,len(maptimes)+1):
	    pl.bar(i,maptimes[i-1]/1000,width=1.0,color="gray",linewidth=0)
	pl.xlabel("Map Tasks",fontsize=25)
	pl.ylabel("Task Execution Time(s)",fontsize=25)
	pl.show()	
	

	

   
        			   		 

				
