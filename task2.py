# SparkContext is entrypoint to Spark, to connect to Spark clusters and create Spark RDD
from pyspark import SparkContext 
import json 

## For memory and time limits
import sys
from resource import *
import time
import psutil
import os 
import itertools
import csv 

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

# SparkContext(Master with number of partitions ,AppName)
sc = SparkContext('local[*]','HW2_Task2') 

# Change logger level to print only ERROR logs
sc.setLogLevel("ERROR")


def refractorOutput(items, length, result):
    if len(items) > length:
        result = result[:-1] + "\n\n"

    result = result + "('" + str(items[0]) + "')," if len(items) == 1 else result + str(items) + ","
    return result

def getOutput(inputList, size , outputVal):
    sortedList = sorted(sorted(inputList), key=len)
    #print("SORTED list: ",sortedList)
    for x in sortedList:
        outputVal = refractorOutput(x, size, outputVal)
        size = len(x)
    return outputVal


def applySON(items):
    prevItemSet = dict()
    result = list()
    # now we again check the original list
    for r in tempRDDList:
        if isinstance(items, tuple):
            if set(items).issubset(r):
                if items in prevItemSet and prevItemSet[items] < input_support:
                    prevItemSet[items] += 1
                elif items not in prevItemSet:
                    prevItemSet[items] = 1
        else:
            prevItemSet = dict() 
            for i in r:
                if i not in prevItemSet.keys():
                    prevItemSet[i] = 1
                else:
                    if prevItemSet[i] < input_support: 
                        prevItemSet[i] = prevItemSet[i] + 1

    for k, v in prevItemSet.items():
        result.append((k, v))
    return result


def countCandidateForLargeSubset(items, length):
    candidates = list()
    for i in items:
        for j in items:
            tup = tuple(sorted(set(i + j)))
            if len(tup) == length:
                # Not for singleton sets only 
                if tup not in candidates:
                    prev = list(itertools.combinations(tup, length - 1))
                    val = True
                    for p in prev:
                        if p not in items:
                            val = False
                            break
                    if val:
                        candidates.append(tup)
    
    return candidates

def createItemCountDictForLargeSubset(buckets, temp,threshold):
    itemCount = dict()
    for b in buckets:
        for t in temp:
            if set(t).issubset(b):
                if t not in itemCount:
                    itemCount[t] = 1
                elif t in itemCount and itemCount[t] < threshold:
                    itemCount[t] = itemCount[t] + 1
    #print("completed itemcountdict")                
    return itemCount

def getFinalDualCandidates(dualList, basket_list, threshold):
    # Now we find frequency of all items 
    dual_item_count = dict()
    for i,j in dualList:
        for basket in basket_list:
            if i in basket and j in basket:
                if (i,j) in dual_item_count:
                    dual_item_count[(i,j)] = dual_item_count[(i,j)] + 1
                else:
                    dual_item_count[(i,j)] = 1
    return dual_item_count

def filterCandidates(dictionary,threshold):
    #print("Now printing dual items: ",dual_item_count)   
    # Now we filter items again and add them to the final candidate list and another list to re-use these values again
    items = list()
    for key,val in dictionary.items():
        if val >= threshold:
            #print("we appended keys: ",key)
            items.append(key)
            final_candidates_list.append(  (key, 1)  )                 
    return items


def getFinalCandidates(soloCandidates):
    # Here we just refractor the singleton candidates and put them in form (F,1)
    for sc in soloCandidates:
        final_candidates_list.append(((sc,),1) ) 
        

def filterAndGetSingleItemCandidates(single_item_counts,threshold):
    # 3. Now we need to filter out the items with less value than threshold , this is the right 
    filtered_dict = {k:v for k,v in single_item_counts.items() if v >= threshold}
    
    # 4. Get candidate pairs
    candidate_pairs = list(filtered_dict.keys()) # These so far have only single frequent values
    #print("Candidate pairs: ",candidate_pairs) ,

    return candidate_pairs

def applyAprioriOnChunks(baskets):
    # we get a partition here which may have one or many baskets
    basket_list = list(baskets)
    items_list = list()
    #print("Baskets : ",basket_list)

    # Determine threshold value, p * s
    threshold = (float) ((len(basket_list) / no_of_baskets) * input_support)

    # Now we get frequency of all individual items
    single_item_counts = dict()
    for basket in basket_list:
        for i in basket:
            if i in single_item_counts:
                single_item_counts[i] = single_item_counts[i] + 1
            else:
                single_item_counts[i] = 1

    # Add these singletons elements to final candidate list  
    soloCandidates = filterAndGetSingleItemCandidates(single_item_counts,threshold)   
    getFinalCandidates(soloCandidates)   
    items_list.append(soloCandidates)
    #print("Final candidates list now of size 1: ",final_candidates_list) 

    # Now we create pairs from singleton items
    dualList = list(itertools.combinations(sorted(soloCandidates), 2))
    items_list = filterCandidates(getFinalDualCandidates(dualList, basket_list, threshold),threshold)
    #print("Final candidates list now of size 2: ",[final_candidates_list])  
    #print("items of size 2 : ",items_list)  
    
    set_size = 3
    # Now we generate subsets of more than 2 element - triplets
    while( len(items_list) != 0) :
        # We first decide how many subsets to create 
        tempList = countCandidateForLargeSubset(items_list, set_size)
        #for subset, dict filter condition is differeny
        tempDict = createItemCountDictForLargeSubset(basket_list,tempList,threshold) 
        items_list = filterCandidates(tempDict, threshold)
        set_size = set_size + 1


    return [final_candidates_list]


startTime = time.time()

# Read all arguments from command line
args=sys.argv
input_filter = int(args[1])
input_support = int(args[2])
input_file_path = str(args[3]) 
output_fle_path = args[4]



# Common variables 
no_of_baskets = 0
final_candidates_list = []
temp_path = "temp.csv"

#3. Process customer_product file to get case1 market basket model
# Create a RDD from given data
dataRDD = sc.textFile(input_file_path)
# Remove the CSV file header 
file_header = dataRDD.first()
dataRDD = dataRDD.filter(lambda row : row != file_header) 
# Building the case1 Market Basket Model
temp_rdd = dataRDD.map(lambda x:x.split(",")).map(lambda x: (x[0].strip('"'),x[1].strip('"'),int(x[5].strip('"'))))
li = []
for x in temp_rdd.collect():
    li.append(str(x[0] +"-"+x[1]+","+str(x[2])))
new_file = sc.parallelize(li)
tempRDD = new_file.map(lambda x: (x.split(",")[0],x.split(",")[1])).groupByKey().mapValues(set).filter(lambda x: len(x[1])>input_filter).map(lambda x:x[1]).persist()

# 0 : TRANSACTION_DT
# 1 : CUSTOMER_ID
# 5 : PRODUCT_ID


with open(temp_path,"w") as temp:
    temp.write("DATE-CUSTOMER_ID,Product_ID\n")
    for x in temp_rdd.collect():
        temp.write(x[0]+"-"+x[1]+","+str(x[2])+"\n")
new_file = sc.textFile(temp_path)
file_header = new_file.first()

tempRDD = new_file.filter(lambda line:line!=file_header).map(lambda x: (x.split(",")[0],x.split(",")[1])).groupByKey().mapValues(set).filter(lambda x: len(x[1])>input_filter).map(lambda x:x[1]).persist()

# Now we apply SON algorithm
no_of_baskets = len(tempRDD.collect())
tempRDDList = tempRDD.collect()

intermediateRDD = tempRDD.mapPartitions(applyAprioriOnChunks).flatMap(lambda item: item).reduceByKey(lambda x, y: x + y).persist()
candidates_rdd_list = intermediateRDD.map(lambda x:x[0]).collect()
candidates_result = getOutput(candidates_rdd_list, 1 , "")[:-1] # since we are getting extra , at the last
#print("This is the final output from Apriori: ",intermediateRDD.collect())

## Map2 and Reduce2 for SON
frequent_itemsets_rdd = intermediateRDD.map(lambda item: applySON(item[0])).flatMap(lambda item: item).filter(
    lambda item: item[1] >= input_support).map(lambda item: item[0]).collect()
#print("Frequenct itemset RDD: ",frequent_itemsets_rdd)    
frequent_itemsets_result = getOutput(frequent_itemsets_rdd, 1 , "")[:-1]
#print("frequ: ",frequent_itemsets_result)

# ##########
## File output
file_record_1 = "Candidates:\n"+candidates_result
file_record_2 = "Frequent Itemsets:\n"+frequent_itemsets_result

with open(output_fle_path, "w") as outfile:
    outfile.write(file_record_1)
    outfile.write("\n\n")
    outfile.write(file_record_2)

endTime = time.time()
print("Duration: ",(endTime-startTime))

