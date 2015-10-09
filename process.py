#!/bin/python
import matplotlib.pyplot as pl

f = open("syslog","r")

start1 = False
start2 = False
hostToUnit = {}
hostToSpeed = {}
hostToRatio = {}
hostToBlock={}
excutionTime = 0.1
excutionRatio = 0.1
block = 0
speed=0
ratio=0
unit=0
host2 = ""
for line in f.readlines():
    if "/BlockNum" in line:
	start2 = False
	continue
    if "BlockNum" in line:
	start2 = True
	continue
    if start2 is True and "TaskDataProvision" in line:
	if "ratio" in line:
	    ratio = float(line.strip().split()[-1].split(":")[-1])
	if "speed" in line:
	    speed = float(line.strip().split()[-1].split(":")[-1])		
	if "unit" in line:
	    unit = float(line.strip().split()[-1].split(":")[-1])	
	if "block" in line:
	    block = float(line.strip().split()[-1].split(":")[-1])
	if "node" in line:	    
	    host2  = line.strip().split()[-1].split(":")[-1]
	    if hostToBlock.get(host2):
		hostToBlock[host2].append(block)
	    else:
		hostToBlock[host2]=[]
		hostToBlock[host2].append(block)

	    if hostToSpeed.get(host2):
		hostToSpeed[host2].append(speed)
	    else:
		hostToSpeed[host2]=[]
		hostToSpeed[host2].append(speed)

	    if hostToRatio.get(host2):
		hostToRatio[host2].append(ratio)
	    else:
		hostToRatio[host2]=[]
		hostToRatio[host2].append(ratio)

	    if hostToUnit.get(host2):
		hostToUnit[host2].append(unit)
	    else:
		hostToUnit[host2]=[]
		hostToUnit[host2].append(unit)




		


sortedKeys = hostToSpeed.keys()
sortedKeys.sort()


print hostToBlock
#pl.figure(1)
#pl.plot(hostToBlock["s2"])
#pl.figure(2)
#pl.plot(hostToRatio["s2"])


pl.figure(1)
for key in sortedKeys:
    pl.plot(hostToSpeed[key])

pl.figure(2)
for key in sortedKeys:
    pl.plot(hostToRatio[key])

pl.figure(3)	
for key in hostToBlock.keys():
   pl.plot(hostToBlock[key])

pl.figure(4)	
for key in hostToUnit.keys():
   pl.plot(hostToUnit[key])

pl.show()	
