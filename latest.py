import re
import time, os
from xml import dom
import xml.etree.ElementTree as ET
from xml.dom.minidom import Node
from xml.dom import minidom
import collections
from io import StringIO
import sys
from xml.dom.minidom import parseString
import time
import csv



def utf8len(s):
    return len(s.encode('utf-8'))


def traverse(root,myDict):
    global f
    if root.childNodes:
        for node in root.childNodes:
            if node.nodeType == Node.ELEMENT_NODE:
                if (node.tagName == "Style"):
                    continue
                elif (
                        node.tagName == 'color' or node.tagName == 'PolyStyle' or node.tagName == 'LineStyle' or node.tagName == 'width'):
                    continue
                if (node.tagName == "Data"):
                    f = "Data" + list(node.attributes.keys())[0]
                    for elem in node.attributes.values():
                        f = f + elem.firstChild.data
                    if (node.hasChildNodes()):
                        y = 0
                        for cn in node.childNodes:
                            if (y == 0):
                                myDict[f].append(cn.firstChild.nodeValue)
                            y = y + 1
                elif (node.tagName == 'value'):
                    continue
                else:
                    prev = node
                    for nn in node.childNodes:
                        if (nn.nodeType == Node.TEXT_NODE):
                            myDict[prev.tagName].append(nn.wholeText)
                            break
                        else:
                            prev = nn
            traverse(node,myDict)


f_list = os.listdir("./")
total_vol =0
for cur in f_list:
    if (cur.endswith('.kml')):
        flag = 0
        try:
            tree = minidom.parse(cur)
            root = tree.documentElement
        except:
            flag = 1
        if (flag == 1):
            print(cur, " is not validated file ")
        else:
            print("parsing file ",cur)
            name = os.path.basename(cur)
            ws = name.split('_')
            myDict = collections.defaultdict(list)
            traverse(root,myDict)
            for key,val in myDict.items():
                if key!='Datanametotal':
                    my_list = val[0].split('-')
                    print(my_list)
                    if(len(my_list)==5):
                        store=[]
                        store.append(my_list[0])
                        store.append(my_list[1])
                        store.append(ws[1])
                        name_of_file = my_list[3]
                        # work_list = os.listdir("./Working")
                        size_of_file =0
                        if my_list[1]!='text':
                            folder = './Working/'
                            print("heyys",my_list[3])
                            if(my_list[3].endswith('.jpeg') or my_list[3].endswith('.jpg')):
                                folder = folder + "SurakshitImages/"
                            elif (my_list[3].endswith('.png')):
                                folder = folder + 'SurakshitMap/'
                            elif (my_list[3].endswith('.mp4')):
                                folder = folder +'SurakshitVideos/'
                            elif (my_list[3].endswith('.3gp')):
                                folder = folder + 'SurakshitAudio/'
                            if os.path.isfile(folder+my_list[3]):
                            	size_of_file = os.path.getsize(folder+my_list[3])
                        else:
                            size_of_file = utf8len(my_list[3])
                        # total_vol += size_of_file
                        # store.append(size_of_file)
                        
                        total_vol += size_of_file
                        store.append(size_of_file)
                        # print("diff = ",sz)
                        with open(ws[1]+"_info"+'.csv','a') as wrfile:
                            writer = csv.writer(wrfile)
                            writer.writerow(store)
                            wrfile.close()
                    else:
                        print(my_list)


sz=os.path.getsize('./Working'+'/SurakshitDiff/')
total_vol +=sz
print(total_vol)
