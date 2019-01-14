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




start_time={}
end_time={}
volume={}
store =[]
flag={}
ip_id={}
file_store={}

def takeSecond(elem):
	return time_parse(elem[1])

def each_node(file_name):
	global start_time
	global end_time
	global store
	test_file = open("test_file.txt",'a')
	logfile=open(file_name,'r')
	logread = csv.reader(logfile)
	if logread is not None:
		for row in logread:
			if row[1]==' PEER_DISCOVERED':
				if(row[2] not in start_time):
					start_time[row[2]]=row[0]
					ip_id[row[3]]=row[2]
					flag[row[2]]=1
					if  row[2]==' 7876044546':
						print("Dis",row[3],row[2])
						test_file.write("Dis "+row[3]+" "+row[2]+" "+row[0]+"\n")
				else:
					if row[3] not in ip_id:
						ip_id[row[3]]=row[2]
						flag[row[2]] +=1
						if  row[2]==' 7876044546':
							print("Dis",row[3],row[2])
							test_file.write("Dis "+row[3]+" "+row[2]+" "+row[0]+"\n")
			elif row[1]==' PEER_LOST':
				if row[3] in ip_id:
					if  row[2]==' 7876044546':
						print("lost",row[3],row[2])
						test_file.write("lost "+row[3]+" "+row[2]+" "+row[0]+"\n")
					ip_id.pop(row[3],None)
					
					if row[2] in flag:
						if(flag[row[2]]>=1):
							end_time[row[2]]=row[0]
							flag[row[2]] -= 1
						if flag[row[2]]==0:
							end_time[row[2]]=row[0]
							diff = time_parse(end_time[row[2]])-time_parse(start_time[row[2]])
							mlist =[]
							mlist.append(row[2])
							mlist.append(start_time[row[2]])
							mlist.append(end_time[row[2]])
							mlist.append(str(diff))
							my_val=0
							if row[2] in volume:
								mlist.append(volume[row[2]])
								my_val = volume[row[2]]
								volume.pop(row[2],None)
							else:
								mlist.append(0)
							if(diff!=0):
								df = str(diff)
								sm = 0 
								ll = df.split(':')
								if len(ll)==3:
									sm = 3600*int(ll[0])+60*int(ll[1])+int(ll[2])
								if sm!=0:
									mlist.append(my_val/sm)
								else:
									mlist.append("undefined")
							else:
								mlist.append("undefined")
							store.append(mlist)
							start_time.pop(row[2],None)
							end_time.pop(row[2],None)
							flag.pop(row[2],None)
					else:
						print("No flaggggggggg")
			elif row[1]==' STOP_FILE_DOWNLOAD':
				value_add =0 
				if(row[3] not in file_store):
					file_store[row[3]]=int(row[4])
					value_add = int(row[4])
				else:
					value_add = int(row[4])-file_store[row[3]]
					file_store[row[3]]=int(row[4])
				# print(file_store[row[3]],value_add)
				if('.' not in row[6]):
					if(row[6] in volume):
						volume[row[6]] += int(value_add)
					else:
						volume[row[6]] = int(value_add)
				else:
					if(row[6] in ip_id):
						if(ip_id[row[6]] in volume):
							volume[ip_id[row[6]]] += int(value_add)
						else:
							volume[ip_id[row[6]]] = int(value_add)
					else:
						print('Not yet detected')
	test_file.close()

	
	
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
		fg= name.split('_')
		ns=name.split('-')
		if(ns[0]=="psyncLog"):
			print(cur_file)
			each_node(cur_file)
			ff_name = name
			
store.sort(key=takeSecond)
print(store)
fg_name = os.path.basename(ff_name).split('-')[1]
write_file = fg_name+"_"+"connection_time"+".csv"
with open(write_file,'a') as wrfile:
	writer = csv.writer(wrfile)
	writer.writerows(store)
	wrfile.close()
