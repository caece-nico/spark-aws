from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('readFile')
spark = SparkContext.getOrCreate(conf=conf)

text = spark.textFile("/mnt/d/Proyectos/Tutorial-SparkAWS/data/sampletext.txt")

final = text.collect()

print(final)