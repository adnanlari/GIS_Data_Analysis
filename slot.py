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




initial = '0'
cflag=0
node_contact={}
total_list=[]
nodes_vol={}
nodes_time={}
slot=0
wr=0

def takeSecond(elem):
	return time_parse(elem[1])



def contact(file_name):
	global wr
	global slot
	global total_vol
	global cflag
	global node_contact
	global initial
	global total_list
	global nodes_time
	global nodes_vol
	logfile=open(file_name,'r')
	logread = csv.reader(logfile)
	if logread is not None:
		for row in logread:
			if cflag==0:
				initial = row[1]
				cflag =1
				node_contact[row[0]]=1
				nodes_vol[row[0]]=int(row[4])
				df = str(row[3]).split(':')
				sm = 3600*int(df[0])+60*int(df[1])+int(df[2])
				nodes_time[row[0]] = sm
					
			else:
				current = row[1]
				diff = time_parse(current)-time_parse(initial)
				ll = str(diff).split(',')
				if(len(ll)==1):
					df = str(diff).split(':')
					sm = 3600*int(df[0])+60*int(df[1])+int(df[2])
					if(sm>1800):
						print(current,initial)
						initial=row[1]
						data_rate=0
						cur =[]
						dd=[]
						slot +=1
						for k,v in node_contact.items():
							data_rate = nodes_vol[k]/nodes_time[k]
							cur=[]
							cur.append(slot)
							cur.append(k)
							cur.append(data_rate)
							cur.append(nodes_time[k])
							dd.append(cur)
						print(dd)
						with open('data_rate.csv','a') as wrfile:
							writer = csv.writer(wrfile)
							for u in range(len(dd)):
								writer.writerow(dd[u])
							wrfile.close()
						xx=0
						sm = sm -1800
						print("sm = ",sm)
						if sm%1800==0:
							xx = (sm/1800)-1
						else:
							xx=math.floor(sm/1800)
						print("xx = ",xx)
						for u in range(xx):
							cur=[]
							slot +=1
							cur.append(slot)
							with open('data_rate.csv','a') as wrfile:
								writer = csv.writer(wrfile)
								writer.writerow(cur)
								wrfile.close()
							
						node_contact={}
						nodes_vol={}
						nodes_time={}
						node_contact[row[0]]=1
						nodes_vol[row[0]]=int(row[4])
						df = str(row[3]).split(':')
						sm = 3600*int(df[0])+60*int(df[1])+int(df[2])
						nodes_time[row[0]] = sm
						wr=1
					else:
						wr=0
						node_contact[row[0]]=1
						if row[0] in nodes_vol:
							nodes_vol[row[0]] += int(row[4])
							
						else:
							nodes_vol[row[0]]=int(row[4])
							
						if row[0] in nodes_time:
							df = str(row[3]).split(':')
							sm = 3600*int(df[0])+60*int(df[1])+int(df[2])
							nodes_time[row[0]] += sm
							
						else:
							df = str(row[3]).split(':')
							sm = 3600*int(df[0])+60*int(df[1])+int(df[2])
							nodes_time[row[0]] = sm
				else:
					print("I am getting more than one day gap")
					initial=row[1]
					data_rate=0
					cur =[]
					dd=[]
					slot +=1
					for k,v in node_contact.items():
						data_rate = nodes_vol[k]/nodes_time[k]
						cur=[]
						cur.append(slot)
						cur.append(k)
						cur.append(data_rate)
						cur.append(nodes_time[k])
						dd.append(cur)
					print(dd)
					with open('data_rate.csv','a') as wrfile:
						writer = csv.writer(wrfile)
						for u in range(len(dd)):
							writer.writerow(dd[u])
						wrfile.close()
					node_contact={}
					nodes_vol={}
					nodes_time={}
					node_contact[row[0]]=1
					nodes_vol[row[0]]=int(row[4])
					df = str(row[3]).split(':')
					sm = 3600*int(df[0])+60*int(df[1])+int(df[2])
					nodes_time[row[0]] = sm
					wr=1
					


	if wr==0:
		print("I am getting here ")
		cur =[]
		dd=[]
		slot +=1
		for k,v in node_contact.items():
			data_rate = nodes_vol[k]/nodes_time[k]
			cur=[]
			cur.append(slot)
			cur.append(k)
			cur.append(data_rate)
			cur.append(nodes_time[k])
			dd.append(cur)
		print(dd)
		with open('data_rate.csv','a') as wrfile:
			writer = csv.writer(wrfile)
			for u in range(len(dd)):
				writer.writerow(dd[u])
			wrfile.close()

							
						







	
	
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
			contact(cur_file)

for j in range(len(total_list)):
	print(total_list[j])
	print('*************************************************')
			

