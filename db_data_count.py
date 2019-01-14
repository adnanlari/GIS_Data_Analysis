import csv
import datetime
import sys
import json
from dateutil import parser
import os
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import statistics





def time_parse(datestring):
	return datetime.datetime.strptime(datestring,"%Y.%m.%d.%H.%M.%S")+datetime.timedelta(days=1)


def takeSecond(elem):
	return time_parse(elem[1])

	
total =0

phonebook={}
other = open('name.csv','r')
otherlog = csv.reader(other)
if otherlog is not None:
	for row in otherlog:
		phonebook[row[0]]=1


ph2={}

f_list = os.listdir("./")
f_list.sort()
for u in range(len(f_list)):
	name = os.path.basename(f_list[u])
	if(name.endswith('connection_time.csv')):
		logfile = open(f_list[u],'r')
		logread = csv.reader(logfile)
		if logread is not None:
			for row in logread:
				if row[0].strip() in phonebook:
					total += int(row[4])
					ph2[row[0].strip()]=1
		break

for key,v in ph2.items():
	print(key)
print("*****************************************************************")
print(total)