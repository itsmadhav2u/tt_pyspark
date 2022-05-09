from pyspark import SparkContext

sc = SparkContext("local[*]", "Count Log Level Using GroupByKey")

sc.setLogLevel("ERROR")

baseRDD = sc.textFile("input/logSample.txt")

pairedRDD = baseRDD.map(lambda line: (line.split(":")[0], line.split(":")[1]))

groupedRDD = pairedRDD.groupByKey()

outputRDD = groupedRDD.map(lambda pair: (pair[0], len(pair[1])))

for pair in outputRDD.collect():
    print(pair)