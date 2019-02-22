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
import math





def time_parse(datestring):
	return datetime.datetime.strptime(datestring,"%Y.%m.%d.%H.%M.%S")+datetime.timedelta(days=1)





my_list=[]
start_time=''
end_time=''
slot_no=0
cur_slot=1
present_time =''

def takeSecond(elem):
	return time_parse(elem[1])

def time_calculator(time_given):
	tm = time_parse(time_given)
	rt = str(tm).split(',')
	pt = rt[0].split(' ')
	ymd = pt[0].split('-')
	hms = pt[1].split(':')
	total_time = 24*3600*int(ymd[2])+3600*int(hms[0])+60*int(hms[1])+int(hms[2])
	return total_time




def get_slot(file_name):
	global slot_no
	global start_time
	global end_time
	global cur_slot
	global my_list
	logfile = open(file_name,'r')
	logread = csv.reader(logfile)
	if logread is not None:
		for row in logread:
			if start_time=='':
				start_time = row[1]
				break
	fp=0
	initial = start_time
	logf=open(file_name,'r')
	logread = csv.reader(logf)
	if logread is not None:
		for row in logread:
			tmp_slot = cur_slot
			tdiff = time_calculator(row[1])-time_calculator(initial)
			predicted_slot=0
			if fp==0:
				predicted_slot=1
				fp+=1
			else:
				predicted_slot=int(tdiff/1800) +1
			tmp_slot = predicted_slot
			
			ac_gap = time_calculator(row[2])-time_calculator(row[1])
			while(ac_gap>=1800):
				print("ac_gap ",ac_gap)
				ll=[]
				vol = 1800*float(row[5])
				ll.append(tmp_slot)
				ll.append(row[0])
				ll.append(vol)
				ll.append(1800)
				ac_gap = ac_gap-1800

				my_list.append(ll)
				tmp_slot+=1
			if(ac_gap<1800 and ac_gap!=0):
				ll=[]
				vol = ac_gap*float(row[5])
				ll.append(tmp_slot)
				ll.append(row[0])
				ll.append(vol)
				ll.append(ac_gap)

				my_list.append(ll)
			















	
		



		

		


							
						







	
	
ff_name=""
f_list = os.listdir("./")
f_list.sort()
for cur_file in f_list:
	ll=0
	try:
		n_file = open("./"+cur_file,'r')
	except:
		ll +=1
	if ll==0:
		name=os.path.basename(n_file.name)
		if(name.endswith('connection_time.csv')):
			print(cur_file)
			get_slot(cur_file)
csvfile = 'curdata_file.csv'
with open(csvfile,'a') as wrfile:
	writer = csv.writer(wrfile)
	writer.writerows(my_list)
	wrfile.close()


for i in range(len(my_list)):
	print(my_list[i])

			

