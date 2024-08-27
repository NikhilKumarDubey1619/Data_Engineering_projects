from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("Friends by average")
sc = SparkContext(conf=conf)

def segragate(l):
    split = l.split(',')
    age = int(split[2])
    nof = int(split[3])
    return age,nof

lines = sc.textFile("file:///SparkCourse/Friends_Counter/fakefriends.csv")
rdd = lines.map(segragate)

totalsbyage =  rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x, y: (x[0] + y[0] , x[1] + y[1] ))
avgbyage = totalsbyage.mapValues(lambda x:x[0]/x[1])
results = totalsbyage.collect()
for result in results:
    print(result)