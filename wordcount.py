#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import re
from operator import add
from pyspark import SparkContext
import math




def statistic_df_time(input_line):  
    df = ''
    t = ''
    ut = ''    
    mat = re.compile('').split(input_line)
    for i in mat:
        if re.search(r'\d\d:\d\d:\d\d',i):
            tempmat = re.compile(':').split(i)
            tempmat2 = re.compile(' ').split(tempmat[0])
            if len(tempmat2) > 1:
                t = tempmat2[1]
        if re.search(r'107~',i):
            tempmat = re.compile(r'~').split(i)
            df = tempmat[1]
        if re.search(r'120~',i):
            tempmat = re.compile(r'~').split(i)
            ut = round(float(tempmat[1])/1000) + 1

    if df != '' and t != '':
        return [df+'~time~'+t,df+'~usedTime~'+str(ut)]
    else:
        return ['df-noneTime']


def statistic_allkey(input_line):
    mat = re.compile("").split(input_line)
    return mat



def statistic_appType(input_line):
    app = ''
    t = ''
    mat = re.compile('').split(input_line)
    for i in mat:
        if re.search(r'\d\d:\d\d:\d\d',i):
            tempmat = re.compile(':').split(i)
            tempmat2 = re.compile(' ').split(tempmat[0])
            if len(tempmat2) > 1:
                t = tempmat2[1]
        if re.search(r'113~',i):
            tempmat = re.compile(r'~').split(i)
            app = tempmat[1]
        if re.search(r'120~',i):
            tempmat = re.compile(r'~').split(i)
            ut = round(float(tempmat[1])/1000) + 1
    if app != '' and t != '':
        return [app+'~time~'+t,app+'~usedTime~'+str(ut)]
    else:
        return ['app-noneTime']



if __name__ == "__main__":
    

    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount <file>"
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")


    lines = sc.textFile(sys.argv[1], 1)
    

    #
    f = statistic_df_time
    fn = 'statistic_df_time.txt'

    counts = lines.flatMap(f) \
                  .map(lambda x:(x,1)) \
                  .reduceByKey(add) \
                  

    output = counts.collect()
    fp = open(fn, "w")
    for (word, count) in output:
        try:
            fp.write("%s: %i" % (word, count)+'\n')
        except:
            print "`````````````````````````````````````````````Error in print\n"
            continue
    #counts.saveAsTextFile('result')
    #output = counts.collect()
    #output = counts.take(20)
    
    sc.stop()


