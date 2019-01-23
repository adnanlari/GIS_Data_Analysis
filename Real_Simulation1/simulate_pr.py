import csv
import datetime
import sys
import json
from dateutil import parser
import os
import time
# import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import statistics
from pathlib import Path
from influxdb import SeriesHelper
from influxdb import InfluxDBClient
import math


def cal_priority(total,no_of_message,diff_slot):
	cur = no_of_message/total
	cur = -math.log(cur)
	cur = cur*math.exp(diff_slot)
	return cur

def priority_function(elem):
	return elem[6]

def time_parse(datestring):
	return datetime.datetime.strptime(datestring,"%Y.%m.%d.%H.%M.%S")+datetime.timedelta(days=1)





myclient = InfluxDBClient('127.0.0.1', 8086, 'root', 'root', database='simulation')

fld=[]
time_dict={}
class MySeriesHelper(SeriesHelper):
	# """Instantiate SeriesHelper to write points to the backend."""

	class Meta:
		# """Meta class stores time series helper configuration."""

		# The client should be an instance of InfluxDBClient.
		client = myclient

		# The series name must be a string. Add dependent fields/tags
		# in curly brackets.
		series_name = 'prDB1'

		# Defines all the fields in this time series.
		fields = fld

		# Defines all the tags for the series.
		tags = []

		# Defines the number of data points to store prior to writing
		# on the wire.
		bulk_size = 0

		autocommit = True




list_of_files=[]
f_list = os.listdir("./")
file_name = open('data_rate.csv','r')
logread= csv.reader(file_name)
fld.append('slot_no')
fld.append('sn')
fld.append('m_id')
fld.append('m_type')
fld.append('size')
fld.append('tile')
print (fld)
if logread is not None:
	for row in logread:
		if len(row)!=1:
			mobile = str(row[1]).strip()
			current_datarate=float(row[2])
			if int(current_datarate)!=0:
				avail_time = int(row[3])
				mobile_no=""
				y=0
				while(y<len(mobile) and (mobile[y].isdigit() is False)):
					y +=1
				mobile_no=mobile[y:]
				print("**************************************************************************************")
				print("Current Mobile no = ",mobile_no,"slot= ",row[0],"avaiable time",avail_time)
				file_dict={}
				list_of_files=[]
				gen_file=mobile_no+"_g.csv"
				gen=Path("./"+gen_file)
				if gen.is_file():
					genf=open(gen_file,'r')
					genread = csv.reader(genf)
					if genread is not None:
						print("GEN: ",mobile_no)
						for rs in genread:
							if len(rs)!=1:
								if int(rs[0])<=int(row[0]):
									query_st = "select m_id from prDB1 where m_id= '" + rs[6].strip() + "'"
									res = myclient.query(query_st)
									res = list(res.get_points(measurement='prDB1'))
									# print("Query: ",query_st,res)
									if len(res)==0:
										if rs[6].strip() not in file_dict:
											file_dict[rs[6].strip()]=1
											cur_file=rs[6].strip()
											qst="select tile_name from gen1 where file_name = '"+cur_file+"'"
											mrq=myclient.query(qst)
											mrq=list(mrq.get_points(measurement='gen1'))
											cur_tile_name="undefined"
											no_of_message=0
											total =0
											if len(mrq)!=0:
												cur_tile_name=mrq[0]['tile_name']
												next_q_st="select count(file_name) from gen1 where tile_name = '"+cur_tile_name+"'"
												qres=myclient.query(next_q_st)
												qres=list(qres.get_points(measurement='gen1'))
												if len(qres)!=0:
													no_of_message=qres[0]['count']
												curq="select count(file_name) from gen1 where slot_no <= "+row[0].strip()
												# print(curq)
												lst = myclient.query(curq)
												lst =list(lst.get_points(measurement='gen1'))
												if(len(lst)!=0):
													total = lst[0]['count']
											diff_slot = int(rs[0])-int(row[0])
											diff_slot = diff_slot*30
											if total==0:
												my_priority=0
											else:
												my_priority=cal_priority(total,no_of_message,diff_slot)


											print("DEBUG",total,no_of_message,diff_slot,my_priority)


											ll=[]
											ll.append(int(row[0]))
											ll.append(int(rs[0]))
											ll.append(rs[6])
											ll.append(rs[3])
											ll.append(rs[7])
											ll.append(rs[1])
											ll.append(my_priority)
											list_of_files.append(ll)
										# size_of_file = float(rs[7])
										# time_req= size_of_file/current_datarate
										# if(time_req<=avail_time):
										# 	avail_time = avail_time - time_req
										# 	dc={}
										# 	dc['slot_no']=row[0]
										# 	dc['m_id']=rs[6].strip()
										# 	dc['m_type']=rs[3]
										# 	dc['size']=int(rs[7])
										# 	dc['tile']=rs[1]
										# 	MySeriesHelper(**dc)
										# 	print("Entered in DB  gen",rs[6],avail_time)
										# else:
											# print("not enough time ",avail_time,time_req)
									else:
										print("File ",rs[6],"is in DB")
								else:
									print("end of slot in generation ",rs[0],avail_time)
									break
							else:
								print("empty row generated")
				else:
					print(mobile_no,"not found gen")
				col_file = mobile_no+"_c.csv"
				col = Path("./"+col_file)
				if col.is_file():
					colf = open(col_file,'r')
					colread = csv.reader(colf)
					if colread is not None:
						print("COL: ",mobile_no)
						for rs in colread:
							if len(rs)!=1:
								if int(rs[0])<=int(row[0]):
									query_st = "select m_id from prDB1 where m_id= '" + rs[2].strip() + "'"
									res = myclient.query(query_st)
									res = list(res.get_points(measurement='prDB1'))
									# print("Query: ",query_st,res)
									if len(res)==0:
										if rs[2].strip() not in file_dict:
											file_dict[rs[2].strip()]=1
											cur_file=rs[2].strip()
											qst="select tile_name from gen1 where file_name = '"+cur_file+"'"
											mrq=myclient.query(qst)
											mrq=list(mrq.get_points(measurement='gen1'))
											cur_tile_name="undefined"
											no_of_message=0
											total =0
											if len(mrq)!=0:
												cur_tile_name=mrq[0]['tile_name']
												next_q_st="select count(file_name) from gen1 where tile_name = '"+cur_tile_name+"'"
												qres=myclient.query(next_q_st)
												qres=list(qres.get_points(measurement='gen1'))
												if len(qres)!=0:
													no_of_message=qres[0]['count']
												curq="select count(file_name) from gen1 where slot_no <= "+row[0].strip()
												# print(curq)
												lst = myclient.query(curq)
												lst =list(lst.get_points(measurement='gen1'))
												if(len(lst)!=0):
													total = lst[0]['count']
											diff_slot = int(rs[0])-int(row[0])
											diff_slot = diff_slot*30
											if total==0:
												my_priority=0
											else:
												my_priority=cal_priority(total,no_of_message,diff_slot)

											print("DEBUG",total,no_of_message,diff_slot,my_priority)

											ll=[]
											ll.append(int(row[0]))
											ll.append(int(rs[0]))
											ll.append(rs[2])
											ll.append(rs[3])
											ll.append(rs[7])
											ll.append("")
											ll.append(my_priority)
											list_of_files.append(ll)
										# size_of_file = float(rs[7])
										# time_req= size_of_file/current_datarate
										# if(time_req<=avail_time):
										# 	avail_time = avail_time - time_req
										# 	dc={}
										# 	dc['slot_no']=row[0]
										# 	dc['m_id']=rs[2].strip()
										# 	dc['m_type']=rs[3]
										# 	dc['size']=int(rs[7])
										# 	dc['tile']=""
										# 	MySeriesHelper(**dc)
										# 	print("Entered in DB  col",rs[2],avail_time)
										# else:
										# 	print("not enough time ",avail_time,time_req)
									else:
										print("File ",rs[2],"is in DB")
								else:
									print("end of slot in collection ",rs[0],avail_time)
									break
							else:
								print("empty row collected")
				else:
					print(mobile_no,"not found col")
				list_of_files.sort(key=priority_function,reverse=True)
				for kt in range(len(list_of_files)):
					print(list_of_files[kt])
				for ind in range(len(list_of_files)):
					dc={}
					dc['slot_no']=list_of_files[ind][0]
					dc['sn']=list_of_files[ind][1]
					dc['m_id']=list_of_files[ind][2].strip()
					dc['m_type']=list_of_files[ind][3]
					dc['size']=list_of_files[ind][4]
					dc['tile']=list_of_files[ind][5].strip()
					# dc['priority']=list_of_files[ind][5]
					size_of_file = float(dc['size'])
					time_req= size_of_file/current_datarate
					if(time_req<=avail_time):
						qr="select m_id from prDB1 where m_id= '"+dc['m_id']+"'"
						fg = myclient.query(qr)
						fg = list(fg.get_points(measurement='prDB1'))
						if len(fg)==0:
							MySeriesHelper(**dc)
							avail_time= avail_time - time_req
							print(list_of_files[ind][2],"is entered in DB")
						else:
							print("Already , SKIPPING..........................................")
				
			else:
				print('skipping due to datarate',mobile)
		else:
			print("no contacts db")
			





			


		
