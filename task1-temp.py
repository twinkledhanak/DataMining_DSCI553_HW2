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

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

# SparkContext(Master with number of partitions ,AppName)
sc = SparkContext('local[*]','HW2_Task1') 

# Change logger level to print only ERROR logs
sc.setLogLevel("ERROR")

# Read all arguments from command line
args=sys.argv
input_case = int(args[1])
input_support = int(args[2])
input_file_path = args[3]
output_fle_path = args[4]

# Common variables 
no_of_baskets = 0


def filterAndGetCandidatePairs(item_count_dict,threshold):
    # 3. Now we need to filter out the items with less value than threshold , this is the right 
    filtered_dict = {k:v for k,v in item_count_dict.items() if v >= threshold}
    
    # 4. Get candidate pairs
    candidate_pairs = list(filtered_dict.keys()) # These so far have only single frequent values
    #print("Candidate pairs: ",candidate_pairs) , @TODO - Find a good way to print these values

    return candidate_pairs

def generateSubsets(candidates, length):
    subset = list()
    for i in candidates:
        for j in candidates:
            tup = tuple(sorted(set(i + j)))
            if len(tup) == length:
                if tup not in subset:
                    fv = True
                    prev = list(itertools.combinations(tup, length - 1))
                    for p in prev:
                        if p not in candidates:
                            fv = False
                            break
                    if fv:
                        subset.append(tup)
    return subset

def getCandidatePairs(d, threshold, flag_value):
    temp = list()
    for key, value in d.items():
        if value >= threshold:
            temp.append(key)
            final_candidate_list.append((key if flag_value else tuple({key}), 1))
    return temp

def createSubsetsAndCompare(basket_list,candidates,threshold,set_size):
    item_counts = dict()
    if set_size == 2:
        # Generating pairs of size 2
        subset_list = list(itertools.combinations(sorted(candidates), set_size))
    else:
        # We generate subsets of size 3 or more - triplets, etc
        subset_list = generateSubsets(candidates, set_size)

    # Now we comapre all pairs and get their frequency from buckets
    for bkt in basket_list:
        for s in subset_list:
            if set(s).issubset(bkt):
                if s in item_counts and item_counts[s] < threshold:
                    item_counts[s] = item_counts[s] + 1
                elif s not in item_counts:
                    item_counts[s] = 1


    return getCandidatePairs(item_counts, threshold, True)

# Apriori Algorithm
def apply_apriori_old(basket):
    # Count occurence of each single item in entire bucket list
    basket_list = list(basket)
    #print("Master BASKET list:: ",basket_list)

    # 1. Get frequency of all single items
    item_count_dict = {} # to maintain mapping of item -> count 
    for basket_item in basket_list:
        for i in basket_item:
            if i in item_count_dict:
                item_count_dict[i] = item_count_dict[i] + 1 #Increment the count
            else:
                item_count_dict[i] = 1 #Initialize the dict with our item

    #print("SINGLETON ITEM_COUNT_DICT: ",item_count_dict)
    # 2. Determine threshold value, p * s
    threshold = (float) ((len(basket_list) / no_of_baskets) * input_support)
    # 3. Filter by threshold and generate a set of singleton frequent itemsets
    candidates = filterAndGetCandidatePairs(item_count_dict,threshold)
    #print("Candidate pairs are: ",candidates)
    final_candidate_list.append(candidates)

    set_size = 2
    while( len(candidates) != 0 ):
        candidates = createSubsetsAndCompare(basket_list,candidates,threshold,set_size)
        set_size = set_size + 1
    return [final_candidate_list]


def apply_apriori(basket):
    # Count occurence of each single item in entire bucket list
    basket_list = list(basket)
    #print("Master BASKET list:: ",basket_list)
    items_list = list()
    # 1. Get frequency of all single items
    for basket_item in basket_list:
        for i in basket_item:
            items_list.append(i)

    #print("SINGLETON ITEM_COUNT_DICT: ",item_count_dict)
    # 2. Determine threshold value, p * s
    threshold = (float) ((len(basket_list) / no_of_baskets) * input_support)
    # 3. Filter by threshold and generate a set of singleton frequent itemsets
    
    #candidates = filterAndGetCandidatePairs(item_count_dict,threshold)
    #print("Candidate pairs are: ",candidates)
    candidates = getCandidatePairs(getItemCountDict(items_list,threshold), threshold,False)
    final_candidate_list.append(candidates)

    set_size = 2
    while( len(candidates) != 0 ):
        candidates = createSubsetsAndCompare(basket_list,candidates,threshold,set_size)
        set_size = set_size + 1
    return [final_candidate_list]




def getItemCountDict(items, s):
    counts = dict()
    for i in items:
        if i not in counts.keys():
            counts[i] = 1
        else:
            if counts[i] < s:
                counts[i] = counts[i] + 1
    return counts


def son_algo(items):
    itemsets = dict()
    result = list()
    for r in tempRDD:
        if isinstance(items, tuple):
            if set(items).issubset(r):
                if items in itemsets and itemsets[items] < input_support:
                    itemsets[items] += 1
                elif items not in itemsets:
                    itemsets[items] = 1
        else:
            itemsets = getItemCountDict(r, input_support)
    for k, v in itemsets.items():
        result.append((k, v))
    return result


# Create a RDD from given data
dataRDD = sc.textFile(str(input_file_path))
# Remove the CSV file header 
file_header = dataRDD.first()
dataRDD = dataRDD.filter(lambda row : row != file_header) 

# For case 1 , we read the input file in a different format (user -> business1, business2)
if input_case is 1:
    #print("We are in case 1")
    # We start processing from second row here
    #1. Create baskets
    tempRDD = dataRDD.map(lambda row: (row.split(",")[0],row.split(",")[1])).groupByKey().mapValues(set).map(lambda row: row[1]).persist()
    #print(tempRDD.collect())
    no_of_baskets = len(tempRDD.collect())

elif input_case is 2:
    print("We are in case 2")   

# We maintain a list to append final candidate values
final_candidate_list = []

# Now we apply Apriori once for every partition and get the frequent itemset
intermediate_results = tempRDD.mapPartitions(apply_apriori).flatMap(lambda sitem: sitem).reduceByKey(lambda a, b: a + b).persist()
#frequent_itemsets_rdd = candidates_rdd.map(lambda x: son_algo(x[0])).flatMap(lambda x: x).filter(lambda x: x[1] >= support).map(lambda x: x[0]).collect()

frequent_itemsets_rdd = intermediate_results.map(lambda sitem: son_algo(sitem[0])).flatMap(lambda sitem : sitem).filter(lambda sitem: sitem[1] >= input_support).map(lambda sitem: sitem[0])#.collect()
candidates_rdd_list = intermediate_results.map(lambda sitem:sitem[0]).collect()


# ip = [{'99', '100', '101', '97'}, {'102', '97', '103', '99', '105', '98'}, {'97', '98'}, {'102', '101'}, {'99', '97', '101'}, {'99', '97', '98'}, {'102', '107', '108', '105', '101', '98', '99', '100', '103', '106'}, {'99', '97', '101'}, {'99', '97', '98'}, {'102', '100', '101', '98'}, {'102', '97', '101', '99', '103'}, {'102', '97', '104', '99', '103', '98'}, {'99', '97', '98'}, {'97', '98'}, {'102', '107', '108', '101', '98', '100', '105', '106'}, {'97'}, {'99', '100', '101', '98'}, {'99', '97'}, {'102', '97', '98'}]
# apply_apriori(ip)
