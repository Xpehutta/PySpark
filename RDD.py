def prepare_chain(x):
    try:
        return "-".join(list(x)[:x.index(1)-1][::2])
    except:
        return "-".join(list(x)[::2])

rdd = sc.textFile('hdfs:////data/lsml/sga/clickstream.csv') \
        .map(lambda x: x.split("\t"))

header = rdd.first()
data = rdd.filter(lambda x: x != header) 

dataSorted = data.sortBy(lambda x: (x[0], x[1], x[-1]))

dataSortedMapped = dataSorted.map(lambda x: [x[0], x[1], x[2], x[3], x[4], 1 if "error" in x[2].lower() else 0])

dataSortedMappedFiltered = dataSortedMapped.filter(lambda x: x[2] == 'page' or "error" in x[2].lower())

dataSortedMappedFilteredReduced = dataSortedMappedFiltered.map(lambda x: [(x[0], x[1]), (x[3], x[-1])])

dataSortedMappedFilteredReducedByKey = dataSortedMappedFilteredReduced.reduceByKey(lambda x, y: x + y).sortByKey()

dataPreFinal = dataSortedMappedFilteredReducedByKey.mapValues(prepare_chain).map(lambda x: (x[-1], -1))

dataPreFinal.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1]).map(lambda x: (x[0], -x[1])).take(30)

