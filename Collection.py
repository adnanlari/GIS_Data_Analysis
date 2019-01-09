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
	return time_parse(elem[0])


file_store={}
file_info = {}
peer_id={}
store=[]
def each_node(file_name):
	global file_store
	global peer_id
	global store
	logfile=open(file_name,'r')
	logread = csv.reader(logfile)
	if logread is not None:
		for row in logread:
			if row[1]==' PEER_DISCOVERED':
				peer_id[row[3]]=row[2]
			elif row[1]==' STOP_FILE_DOWNLOAD':
				# if(row[3] not in file_store):
				file_store[row[3]]=1
				mlist=[]
				message_type="..........."
				fname = row[3]
				if(fname.split('.')[1]=='txt'):
					message_type = "Text"
				elif(fname.split('.')[1]=='diff'):
					message_type="Diff"
				elif(fname.split('.')[1]=='bgp'):
					message_type="Security"
				elif(fname.split('.')[1]=='jpeg' or fname.split('.')[1]=='jpg'):
					message_type="Image"
				elif(fname.split('.')[1]=='mp4'):
					message_type="Video"
				elif(fname.split('.')[1]=='3gp'):
					message_type="Audio"
				elif(fname.split('.')[1]=='png'):
					message_type="Map"
				mlist.append(row[0])
				mlist.append(message_type)
				# mlist.append(row[3])
				source='...........'
				destination ='...........'
				fname=row[3]
				ls = fname.split('_')
				if(len(ls)==5):
					source = ls[1]
					destination = ls[2]
				elif(len(ls)==4):
					source = ls[2]
				elif(len(ls)==2):
					source =ls[1].split('.')[0]
				mlist.append(source)
				mlist.append(destination)
				if(row[6].split('.')==1):
					mlist.append(row[6])
				else:
					if row[6] in peer_id:
						mlist.append(peer_id[row[6]])
					else:
						mlist.append(row[6])
				mlist.append(row[4])
				file_info[row[3]]=mlist
				# store.append(mlist)
	for key,val in file_info.items():
		store.append(val)
	store.sort(key=takeSecond)
	print(store)
	fname = os.path.basename(file_name).split('-')[1]
	write_file = fname+"_"+"Message"+".csv"
	with open(write_file,'a') as wrfile:
		writer = csv.writer(wrfile)
		writer.writerows(store)
		wrfile.close()


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