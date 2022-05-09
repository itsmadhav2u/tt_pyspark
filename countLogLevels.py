from pyspark import SparkContext

sc = SparkContext("local[*]","Count Log Levels")

sc.setLogLevel("ERROR")

"""
If we run this file it calls main method so if condition satisfies.
If we import and run in other file else block will run.
"""

if __name__ == "__main__":
    logList = ["WARN: Tuesday 4 September 1722",
               "ERROR: Tuesday 4 September 1708",
               "WARN: Tuesday 4 September 1708",
               "ERROR: Tuesday 4 September 1722",
               "WARN: Tuesday 4 September 1722",
               "ERROR: Tuesday 4 September 1708",
               "ERROR: Tuesday 4 September 1722"]

    baseRDD = sc.parallelize(logList)
    print("Inside if block")
else:
    baseRDD = sc.textFile("input/logSample.txt")
    print("Inside else block")

pairRDD = baseRDD.map(lambda line: (line.split(":")[0], 1))

outputRDD = pairRDD.reduceByKey(lambda x,y: x+y)

for pair in outputRDD.collect():
    print(pair)
