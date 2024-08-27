import re
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("Better Counter with RegEx")
sc = SparkContext(conf=conf)

# Applied RegEx to remove punctuations form the split words
def TextNormalization(rdd_lines):
    return re.compile(r'\W+',re.UNICODE).split(rdd_lines.lower())

#In Place of path we can place and text file you want to read
book = sc.textFile("file:///DADE/Spark_Projects/Counting word occurance/Book.txt")
#passing function to a flat map
rdd_line = book.flatMap(TextNormalization)

#this is counterr which picks up distinct words with there occurence counts
counterr = rdd_line.countByValue()

for word,count in counterr.items():
    #to check for some encoding isses
    cleanword = word.encode('ascii','ignore')
    if(cleanword):
        print(cleanword.decode() + " " + str(count))