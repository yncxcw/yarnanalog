#!/bin/python
import matplotlib.pyplot as pl

f = open("syslog1","r")

start1 = False
start2 = False
hostTohdfs = {}
hostToSpeed = {}
hostToRatio = {}
hostToBlock={}
excutionTime = 0.1
excutionRatio = 0.1
block = 0
host1 = ""
host2 = ""
for line in f.readlines():
    if "/inform" in line:
	start1 = False
	continue
    if "inform" in line:
	start1 = True
	continue
    if start1 is True:
	if "excutionSpeed" in line:
	    excutionTime  = float(line.strip().split()[-1].split(":")[-1])
	if "excutionRatio" in line:
	    excutionRatio = float(line.strip().split()[-1].split(":")[-1])
	if "host" in line:
	    host1  = line.strip().split()[-1].split(":")[-1]
	    if hostToSpeed.get(host1):
		hostToSpeed[host1].append(excutionTime)
	    else:
		hostToSpeed[host1] = []
		hostToSpeed[host1].append(excutionTime)
	    if hostToRatio.get(host1):
		hostToRatio[host1].append(excutionRatio)
	    else:
		hostToRatio[host1] = []
		hostToRatio[host1].append(excutionRatio)
    if "/BlockNum" in line:
	start2 = False
	continue
    if "BlockNum" in line:
	start2 = True
	continue
    if start2 is True:
	if "block" in line:
	    block = float(line.strip().split()[-1].split(":")[-1])
	if "node" in line:	    
	    host2  = line.strip().split()[-1].split(":")[-1]
	    if hostToBlock.get(host2):
		hostToBlock[host2].append(block)
	    else:
		hostToBlock[host2]=[]
		hostToBlock[host2].append(block)

		


sortedKeys = hostToSpeed.keys()
sortedKeys.sort()


#pl.figure(1)
#pl.plot(hostToBlock["s2"])
#pl.figure(2)
#pl.plot(hostToRatio["s2"])


pl.figure(3)
for key in sortedKeys:
    pl.plot(hostToSpeed[key])

pl.figure(4)
for key in sortedKeys:
    pl.plot(hostToRatio[key])


pl.figure(5)	
for key in hostToBlock.keys():
   pl.plot(hostToBlock[key])
pl.show()	
