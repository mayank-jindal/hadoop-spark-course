# -*- coding: utf-8 -*-
"""pyspark_rdd.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1xukW3SayAkxTDwh8Jy1A6IU-5nW2Yso6
"""

!wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
!tar -xvf spark-3.1.1-bin-hadoop2.7.tgz
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop2.7"
!pip install findspark
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.master("local[*]").getOrCreate()

sc = spark.sparkContext

my_rdd = sc.parallelize([20,40,50,60,70])

type(my_rdd)

my_rdd.collect()

!wget https://raw.githubusercontent.com/futurexskill/bidata/master/retailstore.csv

!ls

my_csv_rdd = sc.textFile('retailstore.csv')

my_csv_rdd.collect()

type(my_csv_rdd)

my_csv_rdd.first()

my_csv_rdd.take(3)

for line in my_csv_rdd.collect():
  print(line)
  print("hello")

my_csv_rdd_2 = my_csv_rdd.map(lambda x : x.replace("Male","M"))

my_csv_rdd_2.collect()

femaleCustomers=my_csv_rdd_2.filter(lambda x: "Female" in x)

femaleCustomers.collect()

femaleCustomers.count()

words = femaleCustomers.flatMap(lambda line: line.split(","))

words.collect()

words.count()

rdd1 = sc.parallelize(["a","b","c","d","e"])
rdd2 = sc.parallelize(["c","e","k","l"])

rdd1.union(rdd2).collect()

rdd1.union(rdd2).distinct().collect()

rdd1.intersection(rdd2).collect()

my_csv_rdd.collect()

def transformRDD(customer) :
    words =customer.split(",")
    #convert male to 0 and female to 1
    if words[2] == "Male" :
         words[2]="0"
    else :
         words[2]="1"
    #Convert N to 0 and Y to 1 for the purchased value
    if words[4] == "N" :
         words[4]="0"
    else :
         words[4]="1"
    #Convert Country to upper case        
    words[3] = words[3].upper()
    return ",".join(words)

my_csv_transformed = my_csv_rdd.map(transformRDD)

my_csv_transformed.collect()

sampleRDD = sc.parallelize([10, 20, 30,40])

sampleRDD.reduce(lambda a, b: a + b)
