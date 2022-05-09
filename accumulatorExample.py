from pyspark import SparkContext

sc = SparkContext("local[*]", "Count empty lines")

def checkBlankLine(line):
    if line.strip() == "":
        emptyLines.add(1)

baseRDD = sc.textFile("input/sampleFile.txt")

# Integer type Accumulator with initial value 0
emptyLines = sc.accumulator(0)

baseRDD.foreach(checkBlankLine)

print(emptyLines.value)