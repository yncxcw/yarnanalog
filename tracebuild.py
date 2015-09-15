import json
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
	print reducetimes   
 
	maptimes = traceBuilder.map_time()
	print "size maps",len(maptimes)
	print "map times"
	print maptimes

   
        mapToHDFS={}
	mapToGCRatio=[]
	mapToUnitTime={}
	mapToTime={}
	maps = traceBuilder.get_maps()
        for mapTask in maps:
            host = mapTask.get_successAttempt().get_hostname()
            time = mapTask.get_spendTime()
	    gcTime =mapTask.get_counterValue("GC_TIME_MILLIS")
	    cpuTime=mapTask.get_counterValue("CPU_MILLISECONDS")	
            read = mapTask.get_counterValue("HDFS_BYTES_READ")
            ratio = 1.0*read/time*1.0
	    gcratio = 1.0*gcTime/time*1.0
	    mapToGCRatio.append(gcratio)
            if mapToUnitTime.get(host):
                mapToUnitTime[host].append(ratio)
	    else:
		mapToUnitTime[host]=[]
                mapToUnitTime[host].append(ratio)		
            if mapToHDFS.get(host):
	        mapToHDFS[host]= mapToHDFS[host]+read
	    else:
		mapToHDFS[host]=read
	    if mapToTime.get(host):
		mapToTime[host].append(time)
	    else:
		mapToTime[host]=[]
		mapToTime[host].append(time)


        sortedKeys = mapToHDFS.keys()
        sortedKeys.sort()
	print sortedKeys	
	for key in sortedKeys: 
	    print "hdfs read ",key,":  ",mapToHDFS[key]/(1000*1000)
	sortedKeys = mapToUnitTime.keys()
        sortedKeys.sort()
	for key in sortedKeys:
	    pl.plot(mapToUnitTime[key])
	    print "average time",key,":  ",sum(mapToUnitTime[key],0.0)/len(mapToUnitTime[key])
	pl.figure(1)
	pl.show()
        
	locality = traceBuilder.get_mapAttemptLocality();
	#print "locallity"
	#print locality

	startTimes = traceBuilder.map_startTime()

	finishTimes = traceBuilder.map_finishTime()

	#committedHeapBytes = traceBuilder.map_countByKey("COMMITTED_HEAP_BYTES")
	#print "virtual heap"
	#print committedHeapBytes
	#virtualMemoryBytes = traceBuilder.map_countByKey("VIRTUAL_MEMORY_BYTES")
	#print "virtual memory"
	#print virtualMemoryBytes
	#physicalMemoryBytes = traceBuilder.map_countByKey("PHYSICAL_MEMORY_BYTES")
	#print "physical memory"
	#print physicalMemoryBytes
	cpuMilliSeconds=traceBuilder.map_countByKey("CPU_MILLISECONDS")
	cpuGCTime=traceBuilder.map_countByKey("GC_TIME_MILLIS")
	print "gcratio average:   ",sum(mapToGCRatio)/len(mapToGCRatio)
        #pl.plot(maptimes)
        #pl.plot(cpuMilliSeconds)
        #pl.plot(cpuGCTime)
	#pl.plot(mapToGCRatio) 
        #pl.figure(2)
	#pl.show()
		
        #print cpuMilliSeconds
	#spilledRecords = traceBuilder.map_countByKey("SPILLED_RECORDS")
	#print "spilled Records"
	#print spilledRecords
	#combineOutputRecords = traceBuilder.map_countByKey("COMBINE_OUTPUT_RECORDS")
	#print combineOutputRecords
	#combineInputRecords = traceBuilder.map_countByKey("COMBINE_INPUT_RECORDS")
	#print combineInputRecords
	#splitRaw = traceBuilder.map_countByKey("SPLIT_RAW_BYTES")
	#print "split Rwa"
	#print splitRaw
	#mapInRecords = traceBuilder.map_countByKey("MAP_INPUT_RECORDS")
	#print "map input records"
	#print mapInRecords
	hdfsBytesRead = traceBuilder.map_countByKey("HDFS_BYTES_READ")
	print "hdfs bytes read"
	#print hdfsBytesRead
	#print "total"
	#print sum(hdfsBytesRead)/(1024*1024)
	#hdfsBytesWrite= traceBuilder.map_countByKey("HDFS_BYTES_WRITTEN")
	#print "hdfs bytes write"
	#print hdfsBytesWrite
	#mapOuRecords = traceBuilder.map_countByKey("MAP_OUTPUT_RECORDS")
	#print "map output records" 
	#print mapOuRecords
	#
	#fileBytesWritten = traceBuilder.map_countByKey("FILE_BYTES_WRITTEN")
	#print "file bytes written"
	#print fileBytesWritten
 

	#reduceAttemptHosts = traceBuilder.get_reduceAttemptHost()
	#print "reduce attempt hosts"
	#print reduceAttemptHosts


	mapAttempHosts = traceBuilder.get_mapAttemptHost()
	print "map attempt hosts"
	
	mapDict = {}
	
	for item in mapAttempHosts:
		if mapDict.get(item):
			mapDict[item]=mapDict[item]+1
		else:   
			mapDict[item]=1
	print mapDict

	reduceAttempHosts = traceBuilder.get_reduceAttemptHost()
	print "reduce attempt hosts"

	reduceDict = {}
	
	for item in reduceAttempHosts:
		if reduceDict.get(item):
			reduceDict[item]=reduceDict[item]+1
		else:   
			reduceDict[item]=1
	print reduceDict
		

	X =[6*x for x in  range(1,41)]

	#Y = traceBuilder.normalrize(virtualMemoryBytes)
	#Y = traceBuilder.get_mapAttemptHost()[0:40]
	#Z = traceBuilder.normalrize(maptimes)[0:40]

	job_starttime = traceBuilder.job.get_launchTime().__strtoint__()
	start_times = []
	start_times.extend(traceBuilder.map_startTime())
	start_times.extend(traceBuilder.reduce_startTime())



	print traceBuilder.reduce_shufflefinishTime()
	finish_times = []
	finish_times.extend(traceBuilder.map_finishTime())
	finish_times.extend(traceBuilder.reduce_finishTime())

	hosts = []
	hosts.extend(traceBuilder.get_mapAttemptHost())
	hosts.extend(traceBuilder.get_reduceAttemptHost())  

	locality_data = []
	for lo in locality:
		if lo=="R":
			locality_data.append(100)
		else:
			locality_data.append(50)

	#pl.figure(2)
	#draw.draw_tasktime_host(locality_data,mapAttempHosts)

	#pl.figure(2)
	#draw.draw_tasktime_host(maptimes,mapAttempHosts) 
	#pl.figure(2)
	#draw.draw_tasktime_host(reducetimes,reduceAttemptHosts) 
	#pl.figure(3)
	#draw_bar(pl,job_starttime,start_times,finish_times,traceBuilder.reduce_shufflefinishTime(),hosts)
	#draw.draw_tasktime(maptimes)
	#print fileBytesWritten
	#print combineInputRecords
	#print combineOutputRecords
	#print traceBuilder.map_startTime()
	#print traceBuilder.reduce_startTime()

	#pl.bar(X,Z)
	#pl.xticks(X,Y)
	#pl.show()
	#pl.figure(2)
	#pl.plot(X,Z)
	#pl.show()
	

			   		 

				
