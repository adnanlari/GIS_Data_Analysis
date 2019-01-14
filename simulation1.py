import csv
import datetime
import sys
import json
from dateutil import parser
import os
import matplotlib.pyplot as plt
import collections
import numpy as np
import statistics

def convert(datestring):
	year=datestring[0:4]
	mon = datestring[4:6]
	day = datestring[6:8]
	hour = datestring[8:10]
	minute = datestring[10:12]
	second = datestring[12:14]
	return year+"."+mon+"."+day+"."+hour+"."+minute+"."+second




def time_parse(datestring):
	return datetime.datetime.strptime(datestring,"%Y.%m.%d.%H.%M.%S")+datetime.timedelta(days=1)



def takeFirst(elem):
	return time_parse(elem[1])


def takeSecond(elem):
	return time_parse(convert(elem[0]))



it=0
time_diff=30
time_unit='m'
prev_time='0'
total_data=0
sorted_list=[]
db_list=[]
node_list=[]
node_info=collections.defaultdict(list)
cof='0'

def getinfotimewise():
	global cof
	global node_info
	global sorted_list
	global prev_time
	global total_data
	f_list = os.listdir("./")
	for fname in f_list:
		cof ='0'
		prev_time='0'
		sorted_list=[]
		n_file = open("./"+fname,'r')
		name=os.path.basename(n_file.name)
		if name.endswith('info.csv'):
			cl = name.split('_')
			if(cl[0][0]=='v'):
				fgf = open(name,'r')
				flog = csv.reader(fgf)
				if flog is not None:
					for row in flog:
						mm=[]
						mm.append(row[0])
						mm.append(row[1])
						mm.append(row[2])
						mm.append(row[3])
						sorted_list.append(mm)
				sorted_list.sort(key=takeSecond)
				# print(sorted_list)
				# print("break .........................................")
				for row in sorted_list:
					if  cof=='0':
						prev_time = convert(row[0])
						total_data = int(row[3])
						cof='1'
					else:
						cur_time = convert(row[0])
						print(cur_time,prev_time,"prev",row[2])
						diff = time_parse(cur_time) - time_parse(prev_time)
						fg = str(diff).split(':')
						sm=0
						# print(prev_time,cur_time,diff)
						if len(fg)==3:
							sm = 3600*int(fg[0])+60*int(fg[1])+int(fg[2]);
						elif(len(fg)==4):
							sm = 24*3600*int(fg[0])+3600*int(fg[1])+60*int(fg[2])+int(fg[3]);
						if(sm>300):
							node_info[row[2]].append(total_data)
							prev_time=cur_time
							total_data = int(row[3])
						else:
							total_data += int(row[3])

	for key,val in node_info.items():
		print(key,val)

def getfromDB():
	global cof
	global it
	global node_info
	global node_list
	global db_list
	cof ='0'
	fdname=""
	db_info=[]
	prev_time='0'
	total_data=0
	nodes={}
	f_list = os.listdir("./")
	for fname in f_list:
		n_file = open("./"+fname,'r')
		name=os.path.basename(n_file.name)
		if name.endswith('connection_time.csv'):
			fdname = name
			fgf = open(name,'r')
			flog = csv.reader(fgf)
			if flog is not None:
				for row in flog:
					db_info.append(row)
			db_info.sort(key=takeFirst)
			for row in db_info:
				if(row[0].find('mule')==-1):
					if cof=='0':
						nodes['v'+row[0].strip()]=1
						prev_time=row[1]
						if ('v'+row[0].strip() in node_info):
							total_data=int(row[4])
						else:
							total_data=0
						cof ='1'
					else:
						nodes['v'+row[0].strip()]=1
						cur_time = row[1]
						diff = time_parse(cur_time) - time_parse(prev_time)
						print(cur_time,prev_time,diff)
						gh = str(diff).split(',')
						if len(gh)==1:
							fg = str(diff).split(':')
							sm=0
							# print(prev_time,cur_time,diff)
							if len(fg)==3:
								sm = 3600*int(fg[0])+60*int(fg[1])+int(fg[2]);
							elif(len(fg)==4):
								sm = 24*3600*int(fg[0])+3600*int(fg[1])+60*int(fg[2])+int(fg[3]);
							if(sm>300):
								db_list.append(total_data)
								prev_time=cur_time
								data =0
								for k,v in nodes.items():
									print(k,'pppcjcnsnc')
									if(k in node_info and len(node_info[k])>it):
										print('cm')
										data += node_info[k][it]
									# nodes.pop(k,None)
								print(nodes,'ndskwdiiew')
								node_list.append(data)
								nodes.clear()
								print(nodes,'bsuwgduhwidh')
								it +=1
								if ('v'+row[0].strip() in node_info):
									total_data=int(row[4])
								else:
									total_data=0
							else:
								if ('v'+row[0].strip() in node_info):
									total_data+=int(row[4])
				else:
					print('mule is not detected')
	print(db_list)
	print(node_list)

	n_db=[]
	n_node=[]
	cusum=0
	for j in range(len(db_list)):
		cusum += db_list[j]
		n_db.append(cusum)

	cusum=0
	for j in range(len(node_list)):
		cusum += node_list[j]
		n_node.append(cusum)

	print(n_db)
	print(n_node)
	
	ac = fdname.split('_')[0]
	my_storage=[]
	for u in range(len(n_db)):
		ll=[]
		ll.append(ac)
		ll.append(n_db[u])
		ll.append(n_node[u])
		my_storage.append(ll)


	
	with open('final_file.csv','a') as wrfile:
		writer = csv.writer(wrfile)
		writer.writerows(my_storage)
		wrfile.close()
	x_ax=[]
	lt = len(n_db)
	st = 0
	for k in range(lt):
		x_ax.append(st)
		st +=2
	x=np.linspace(x_ax[0],x_ax[-1],50)
	z1=np.polyfit(x_ax,n_node,3)
	f=np.poly1d(z1)
	y=f(x)
	z2=np.polyfit(x_ax,n_db,2)
	f1=np.poly1d(z2)
	y2=f1(x)
	#print(x,y)
	plt.plot(x,y,'r')
	#plt.show()
	plt.plot(x,y2,'b')
	plt.show()









getinfotimewise()
getfromDB()








