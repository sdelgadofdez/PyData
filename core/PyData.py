import sys

from pyspark import SparkConf, SparkContext
import sys



def main(file_name: str) -> None:
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    file = spark_context\
        .textFile(file_name)
    header = file.first()
    
    # TODO: Fix line parsing for csv.
    d = file.filter(lambda line: line != header)\
        .map(lambda line: line.split(','))\
        .map(lambda r: (r[5], r[6]))\
        .groupByKey()\
        .map(lambda x: (x[0], set(x[1])))\
        .mapValues(len)\
        .collect()

    print(d)


if __name__ == "__main__":
    main(sys.argv[1])
>>>>>>> Indicators count implemented
