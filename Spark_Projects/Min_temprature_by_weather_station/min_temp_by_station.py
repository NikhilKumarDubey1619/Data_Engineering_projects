from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("Min Temprature by station id")
sc = SparkContext(conf=conf)

def parsedLine(line):
    fields = line.split(',')
    Stationid = fields[0]
    category = fields[2]
    cel_temp = (float(fields[3]) * 0.18) + 32
    return(Stationid,category,cel_temp)


lines = sc.textFile("file:///DADE/Spark_Projects/Min_temprature_by_weather_station/1800.csv")
parsedresult = lines.map(parsedLine)

min_temp = parsedresult.filter(lambda x : "TMIN" in x[1])
station_temp = min_temp.map(lambda x: (x[0] , x[2]))
#combine by key
min_temp_by_station = station_temp.reduceByKey(lambda x,y : min(x,y))

min_temps = min_temp_by_station.collect()
for result in min_temps:
    print(result[0]+ "\t{:.2f}F".format(result[1]))

