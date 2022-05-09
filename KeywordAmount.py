from pyspark import SparkContext

def loadBoaringWords():
    words = set()
    for line in open("input/boringwords.txt"):
        words.add(line.lower().strip())
    return words


sc = SparkContext("local[*]", "KeywordAmount")

baseRDD = sc.textFile("input/bigdatacampaigndata.csv")

mappedRDD = baseRDD.map(lambda line: (float(line.split(",")[10]), line.split(",")[0]))

wordsRDD = mappedRDD.flatMapValues(lambda value: value.split(" "))

pairRDD = wordsRDD.map(lambda words: (words[1].lower(), words[0]))

boaringWordsRDD = sc.broadcast(loadBoaringWords())

filteredRDD = pairRDD.filter(lambda pair: pair[0] not in boaringWordsRDD.value)

outputRDD = filteredRDD.reduceByKey(lambda x,y : x+y).sortBy(lambda pair: pair[1], False)

sampleOutput = outputRDD.take(10)

for result in sampleOutput:
    print(result)