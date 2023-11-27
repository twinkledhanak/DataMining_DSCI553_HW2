from pyspark import SparkContext
from pyspark import SparkConf
import sys
import os
import time
from itertools import combinations
from collections import defaultdict
from operator import add

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# conf = SparkConf().setAppName("MyApp").setMaster("local[*]").set("spark.executor.memory", "4g")

# sc = SparkContext(conf=conf)
sc = SparkContext('local', 'hw2')

#filt = int(sys.argv[1])
#supp = int(sys.argv[2])
#reviewFilePath = sys.argv[3] #line 24/27
#outputFilePath = sys.argv[4] #line 201/202

filt = 20
supp = 50
reviewFilePath = "ta_feng_all_months_merged.csv"
outputFilePath = "output.txt"

#Load files directly into rdd format
# tf = sc.textFile(r"C:\Users\Aleck Cervantes\Desktop\ta_feng_all_months_merged.csv").map(lambda line: line.split(","))
tf = sc.textFile(f"{reviewFilePath}").map(lambda line: line.split(","))
tf_part = tf.getNumPartitions()


#identify header columns
cols = tf.first()

#remove header columns
tf = tf.filter(lambda line: line != cols)

#4.2.1
tf_core = tf.map(lambda x: (x[0].strip('"'), x[1].strip('"').lstrip('0'), x[5].strip('"').lstrip('0')))

tf_core = tf_core.map(lambda x: ("{}-{}".format(x[0], x[1]), x[2]))

# new_cols = ("DATE-CUSTOMER_ID", "PRODUCT_ID")

# tf_core_clean = tf_core.map(lambda x: Row(col1 = x[0], col2 = x[1])).toDF(new_cols)

# #using csv library because cannot use normal 'Write' function
# with open(r"C:/Users/Aleck Cervantes/Downloads/customer_product.csv", 'w', newline='') as file:
# # with open(f"{outputFilePath}", 'w', newline='') as file:
#     writer = csv.writer(file, delimiter=',')

#     # write the header row
#     writer.writerow(new_cols)

#     # write each row of data to the file
#     for row in tf_core_clean.collect():
#         writer.writerow(row)

#4.2.2
start_time = time.time()

# clean_tf = sc.textFile(r"C:\Users\Aleck Cervantes\Downloads\customer_product.csv").map(lambda line: line.split(","))
# clean_tf = sc.textFile(f"{reviewFilePath}").map(lambda line: line.split(","))

"""
count each date_id and add them together
remove cases that dont have enough purchases to pass filter
"""
tf_filtering = tf_core.map(lambda x: (x[0], 1)).reduceByKey(add)
tf_filtering = tf_filtering.filter(lambda x: x[1] > filt)
tf_filtering = tf_filtering.map(lambda x: x[0])

tf_filt = set(tf_filtering.collect())
clean_tf = tf_core.filter(lambda x: x[0] in tf_filt)
tf_grouped = clean_tf.groupByKey().mapValues(list).map(lambda x: (x[0], sorted(list(x[1]), key=int))).sortByKey()

#collect only the business id's so we may count how many times a user reviewed those businesses
tf_baskets = tf_grouped.map(lambda x: x[1])

def get_candidates(subset_rdd, support): 
    
    freq = []
    """
    Load the partition of the rdd
    get the length of the parition and generate new support
    by multiplying to old support
    """
    subset_rdd = list(subset_rdd)
    sub_b = len(subset_rdd)
    new_support = supp / tf_part
    
    """
    count the number of times a singlet appears in the baskets
    filter out cases that dont meet the support threshold
    """
    item_cnt = defaultdict(int)
    for basket in subset_rdd:
        for item in basket:
            item_cnt[item] += 1
    cand_sing = list(item_cnt.keys())

    freq_sing = [k for k, v in item_cnt.items() if v>=new_support]
    
    freq_sing = sorted(freq_sing)

    freq.append(list(sorted(freq_sing)))
    
    """
    generate new combinations for doublets
    """
    candidates = list(combinations(freq_sing, 2))

    while candidates != []:
        
        new_item_cnt = defaultdict(int)
        for basket in list(subset_rdd):
            for cand in candidates:
                if set(cand).issubset(set(basket)):
                    new_item_cnt[tuple(cand)] += 1        
        
        freq_sets = [k for k, v in new_item_cnt.items() if v>=new_support]
        if freq_sets != []:
            freq.append(sorted(freq_sets))

        
        """
        iterate through combinations (-1 to remove duplcate at end), iterate through same list again and union
        position i with position j until all combinations exhausted
        """
        candidates = []
        for i in range(len(freq_sets) - 1):
            for j in range(i, len(freq_sets)):
                a = set(freq_sets[i]).union(set(freq_sets[j]))
                if len(a) == len(freq_sets[0]) + 1:                    
                    candidates.append(a)
        candidates = [tuple(sorted(i)) for i in candidates]
        candidates = sorted([tuple(sorted(i)) for i in set(candidates)])

        
    return freq



cands = tf_baskets.mapPartitions(lambda x: get_candidates(x, supp))

final_item_cnt = defaultdict(int)

baskets = tf_baskets.collect()
candidates = cands.collect()

for basket in baskets:
    for cand in candidates:
        for item in cand:
            if type(item) == str:
                item = (item,)
            if set(item).issubset(set(basket)):
                final_item_cnt[tuple(item)] += 1 

final_freq_sets = [k for k, v in final_item_cnt.items() if v>=supp]

"""unnest lists from partitions"""

unnested = []
for i in candidates:
    for j in i:
        if type(j) == str:
            j = (j,)
            unnested.append(j)
        else:
            unnested.append(j)

"""
sort final output by tuple length
create a list for all values of that length
when length changes, append previous list and start new list + iterate
"""

sorted_list = sorted(unnested, key = lambda x: len(x))

#4.2.Case1
cand_list = []
nest_test = []
num = 1
for i in sorted_list:
    if len(i) == num:
        nest_test.append(i)
    elif len(i) != num:
        cand_list.append(sorted(set(nest_test)))
        nest_test = []
        nest_test.append(i)
        num += 1
cand_list.append(sorted(nest_test))
        
sorted_list = sorted(final_freq_sets, key = lambda x: len(x))

freq_list = []
nest_test = []
num = 1
for i in sorted_list:
    if len(i) == num:
        nest_test.append(i)
    elif len(i) != num:
        freq_list.append(sorted(nest_test))
        nest_test = []
        nest_test.append(i)
        num += 1
freq_list.append(sorted(nest_test))

# with open(r"C:/Users/Aleck Cervantes/Downloads/task2.txt", 'w') as file:
with open(outputFilePath, 'w') as file:
    file.write('Candidates:\n')
  
    for i in cand_list:
        if len(i[0]) == 1:
        
            for j in range(len(i)):
                if j != len(i) - 1:
                    file.write(f"('{(str(*i[j]))}'),")
                else:
                    file.write(f"('{(str(*i[j]))}')\n\n")
        else:
            for j in range(len(i)):
                if j != len(i) - 1:
                    file.write(str(f"{i[j]}, "))
                else:
                    file.write(str(f"{i[j]}\n\n"))

    file.write('Frequent Itemsets:\n') 

    for i in freq_list:
        if len(i[0]) == 1:
        
            for j in range(len(i)):
                if j != len(i) - 1:
                    file.write(f"('{(str(*i[j]))}'),")
                else:
                    file.write(f"('{(str(*i[j]))}')\n\n")
        else:
            for j in range(len(i)):
                if j != len(i) - 1:
                    file.write(str(f"{i[j]}, "))
                else:
                    file.write(str(f"{i[j]}\n\n"))
run_time = time.time() - start_time

print(run_time)
