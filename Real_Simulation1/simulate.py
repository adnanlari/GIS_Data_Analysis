import csv
import datetime
import sys
import json
from dateutil import parser
import os
import time
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import statistics
from pathlib import Path
from influxdb import SeriesHelper
from influxdb import InfluxDBClient

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
        series_name = 'sDB1'

        # Defines all the fields in this time series.
        fields = fld

        # Defines all the tags for the series.
        tags = []

        # Defines the number of data points to store prior to writing
        # on the wire.
        bulk_size = 0

        autocommit = True

f_list = os.listdir("./")
file_name = open('data_rate.csv','r')
logread= csv.reader(file_name)
fld.append('slot_no')
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
									query_st = "select m_id from sDB1 where m_id= '" + rs[6].strip() + "'"
									res = myclient.query(query_st)
									res = list(res.get_points(measurement='sDB1'))
									# print("Query: ",query_st,res)
									if len(res)==0:
										size_of_file = float(rs[7])
										time_req= size_of_file/current_datarate
										if(time_req<=avail_time):
											avail_time = avail_time - time_req
											dc={}
											dc['slot_no']=row[0]
											dc['m_id']=rs[6].strip()
											dc['m_type']=rs[3]
											dc['size']=int(rs[7])
											dc['tile']=rs[1]
											MySeriesHelper(**dc)
											print("Entered in DB  gen",rs[6],avail_time)
										else:
											print("not enough time ",avail_time,time_req)
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
									query_st = "select m_id from sDB1 where m_id= '" + rs[2].strip() + "'"
									res = myclient.query(query_st)
									res = list(res.get_points(measurement='sDB1'))
									# print("Query: ",query_st,res)
									if len(res)==0:
										size_of_file = float(rs[7])
										time_req= size_of_file/current_datarate
										if(time_req<=avail_time):
											avail_time = avail_time - time_req
											dc={}
											dc['slot_no']=row[0]
											dc['m_id']=rs[2].strip()
											dc['m_type']=rs[3]
											dc['size']=int(rs[7])
											dc['tile']=""
											MySeriesHelper(**dc)
											print("Entered in DB  col",rs[2],avail_time)
										else:
											print("not enough time ",avail_time,time_req)
									else:
										print("File ",rs[2],"is in DB")
								else:
									print("end of slot in collection ",rs[0],avail_time)
									break
							else:
								print("empty row collected")
				else:
					print(mobile_no,"not found col")
			else:
				print('skipping due to datarate',mobile)
		else:
			print("no contacts db")
			





			


		
