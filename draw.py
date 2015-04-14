import matplotlib.pyplot as pl
from operator import itemgetter

	
def draw_tasktime(tasktime):
	X = [x for x in range(1,len(tasktime)+1)]
	Y = tasktime
	pl.bar(X,Y)
	pl.show()
	return

def	draw_tasktime_hosttime():
	return

def draw_tasktime_host(tasktime,taskhost):
		tuple_list=[]
		for i in range(0,len(tasktime)):
			tuple_list.append((tasktime[i],taskhost[i]))	
		tuple_list=sorted(tuple_list,key=itemgetter(1))
		X=[3*x for x in range(1,len(tasktime)+1)]
		Y=[]
		Z=[]
		for i in range(0,len(tasktime)):
			Y.append(tuple_list[i][0])
			Z.append(tuple_list[i][1])
		pl.bar(X,Y)
		pl.xticks(X,Z)
		pl.show()
	
