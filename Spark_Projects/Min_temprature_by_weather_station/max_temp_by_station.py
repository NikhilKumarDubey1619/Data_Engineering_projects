from pyspark import SparkConf,SparkContext

def parseline(lines):
    fields = lines.split(',')
    stationid = fields[0]
    entrytypes = fields[2]
    celcius_temp = (float(fields[3])*0.18) + 32
    return (stationid,entrytypes,celcius_temp)


conf = SparkConf().setMaster("local").setAppName("Max Temprature by staion id")
sc = SparkContext(conf=conf)



rdd_line = sc.textFile("file:///DADE/Spark_Projects/Min_temprature_by_weather_station/1800.csv")
rdd_parse = rdd_line.map(parseline)

rdd_filter = rdd_parse.filter(lambda x:'TMAX' in x[1])
rdd_max = rdd_filter.map(lambda x: (x[0],x[2]))

rdd_max_temp = rdd_max.reduceByKey(lambda x,y: max(x,y))
rdd_final = rdd_max_temp.collect()

for result in rdd_final:
    print(result[0] + "\t{:.2f}F".format(result[1]))


