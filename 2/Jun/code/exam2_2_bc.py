from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Create spark session
spark = SparkSession.builder.appName("Exam 2").getOrCreate()
# Define input path
input_path_world_cups = "/tmp/exam/WorldCups.csv"
input_path_world_cup_matches = "/tmp/exam/WorldCupMatches.csv"
# Load inptu data and consturct dataframe 
world_cups_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path_world_cups)
world_cup_matches_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path_world_cup_matches)

# Generate RDD
world_cups_rdd = world_cups_df.rdd

# RDD 1 (*): Get the most recent year of world cup
print("RDD - Get the most recent year of world cup")
print(world_cups_rdd.max(lambda x: x[0])[0])

# RDD 2 (*): Get row where Country is USA
print("RDD - Get row where Country is USA")
print(world_cups_rdd.filter(lambda x: x[1] == 'USA').collect())

# RDD 3 (*): Get total row count of the input data set
print("RDD - Get total row count of the input data set")
print(world_cups_rdd.count())

# RDD 4 (*): Get year where country is same as winner
print("RDD - Get year where country is same as winner")
print([(_[0], _[1]) for _ in world_cups_rdd.filter(lambda x: x[1] == x[2]).collect()])

# RDD 5 (*): Get total number of matches played
print("RDD - Get total number of matches played")
print(world_cups_rdd.map(lambda x: x[8]).sum())
