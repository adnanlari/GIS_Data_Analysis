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



def deg2num(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(lat_deg)
  n = 2.0 ** zoom
  xtile = int((lon_deg + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
  ll=[]
  ll.append(xtile)
  ll.append(ytile)
  return ll


initial ='0'
slot =0
my_dict={}

def takeSecond(elem):
    return time_parse(elem[2])

def time_parse(datestring):
	return datetime.datetime.strptime(datestring,"%Y.%m.%d.%H.%M.%S")+datetime.timedelta(days=1)

def convert(str):
	rt=""
	rt = str[0:4]+"."+str[4:6]+"."+str[6:8]+"."+str[8:10]+"."+str[10:12]+"."+str[12:14]
	return rt



def contact(file_name):
	global initial
	global slot
	global my_dict
	current =""
	logfile=open(file_name,'r')
	logread = csv.reader(logfile)
	if logread is not None:
		for row in logread:
			if initial=='0':
				initial = row[0]
				my_dict[row[4]]=row
			else:
				current = row[0]
				diff = time_parse(current)-time_parse(initial)
				st = str(diff)
				if ',' not in st:
					df = st.split(':')
					sm = 3600*int(df[0])+60*int(df[1])+int(df[2])
					if sm>1800:
						initial = row[0]
						my_list=[]
						slot +=1
						for k,v in my_dict.items():
							r=[]
							lat_lon= v[3]
							lat = lat_lon.split('_')[0]
							lon = lat_lon.split('_')[1]
							lat = float(lat)
							lon=float(lon)
							llist = deg2num(lat,lon,16)
							st = str(llist[0])+"_"+str(llist[1])
							r.append(slot)
							r.append(st)
							for u in range(len(v)):
								r.append(v[u])
							my_list.append(r)

						my_list.sort(key=takeSecond)
						my_dict={}
						ft = sm -1800
						xx=0
						if(ft%1800==0):
							xx = (ft/1800)-1
						else:
							xx=math.floor(ft/1800)
						print('sm= ',sm,'xx=',xx,'ft= ',ft)
						with open('slot_file_gen.csv','a') as wrfile:
							writer = csv.writer(wrfile)
							writer.writerows(my_list)
							wrfile.close()
						for u in range(xx):
							cur=[]
							slot +=1
							cur.append(slot)
							with open('slot_file_gen.csv','a') as wrfile:
								writer = csv.writer(wrfile)
								writer.writerow(cur)
								wrfile.close()
						my_dict[row[4]]=row
					else:
						my_dict[row[4]]=row
				else:
					initial = row[0]
					my_list=[]
					slot +=1
					for k,v in my_dict.items():
						r=[]
						lat_lon= v[3]
						lat = lat_lon.split('_')[0]
						lon = lat_lon.split('_')[1]
						lat = float(lat)
						lon=float(lon)
						llist = deg2num(lat,lon,16)
						st = str(llist[0])+"_"+str(llist[1])
						r.append(slot)
						r.append(st)
						for u in range(len(v)):
							r.append(v[u])
						my_list.append(r)
					my_list.sort(key=takeSecond)
					my_dict={}
					with open('slot_file_gen.csv','a') as wrfile:
						writer = csv.writer(wrfile)
						writer.writerows(my_list)
						wrfile.close()
					my_dict[row[4]]=row

	if len(my_dict)!=0:
		my_list=[]
		slot +=1
		for k,v in my_dict.items():
			r=[]
			lat_lon= v[3]
			lat = lat_lon.split('_')[0]
			lon = lat_lon.split('_')[1]
			lat = float(lat)
			lon=float(lon)
			llist = deg2num(lat,lon,16)
			st = str(llist[0])+"_"+str(llist[1])
			r.append(slot)
			r.append(st)
			for u in range(len(v)):
				r.append(v[u])
			my_list.append(r)
		my_list.sort(key=takeSecond)
		my_dict={}
		with open('slot_file_gen.csv','a') as wrfile:
			writer = csv.writer(wrfile)
			writer.writerows(my_list)
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
		if(name.endswith('info.csv')):
			contact(cur_file)