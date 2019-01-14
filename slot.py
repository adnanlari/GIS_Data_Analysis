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




initial = '0'
cflag=0
node_contact={}

def takeSecond(elem):
	return time_parse(elem[1])



def contact(file_name):
	global cflag
	global node_contact
	global initial
	logfile=open(file_name,'r')
	logread = csv.reader(logfile)
	if logread is not None:
		for row in logread:
			if cflag==0:
				initial = row[0]
				cflag =1
				if row[1]==' PEER_DISCOVERED':
					node_contact[row[2]]=1
			else:
				current = row[0]
				diff = time_parse(current)-time_parse(initial)
				ll = str(diff).split(',')
				if(len(ll)==1):
					df = str(diff).split(':')
					sm = 3600*int(df[0])+60*int(df[1])+int(df[2])
					if(sm>1800):
						initial=row[0]
						for k,v in node_contact.items():
							print(k)
						print('**********************************************')
						node_contact={}
						if(row[1]==' PEER_DISCOVERED'):
							node_contact[row[2]]=1
					else:
						if(row[1]==' PEER_DISCOVERED'):
							node_contact[row[2]]=1
				else:
					initial = row[0]
					for k,v in node_contact.items():
						print(k)
					print('**********************************************')
					node_contact={}
					if(row[1]==' PEER_DISCOVERED'):
						node_contact[row[2]]=1
					else:
						if(row[1]==' PEER_DISCOVERED'):
							node_contact[row[2]]=1







	
	
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
			contact(cur_file)
			ff_name = name
			

