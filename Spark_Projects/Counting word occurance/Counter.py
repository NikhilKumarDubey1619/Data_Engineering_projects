from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("Word_counter")
sc = SparkContext(conf=conf)


#In Place of path we can place and text file you want to read
book = sc.textFile("file:///DADE/Spark_Projects/Counting word occurance/Book")

#Splits the book lines word by word
rdd_book = book.flatMap(lambda x: x.split())

#Counts the distinct word and count it occurance
words = rdd_book.countByValue()

for word,count in words.items():
    cleanWord = word.encode('ascii','ignore')
    if (cleanWord):
        print(cleanWord,count)