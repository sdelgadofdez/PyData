from pyspark import SparkConf, SparkContext
import sys
import csv
import time


def main(file_name: str) -> None:
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    
    start_computing_time = time.time()
    
    file = spark_context \
        .textFile(file_name)
    header = file.first()

    d = file.filter(lambda line: line != header) \
        .map(lambda line: csv.reader([line], quotechar='"', delimiter=',',
                                     quoting=csv.QUOTE_ALL, skipinitialspace=True).__next__()) \
        .map(lambda r: (r[5], r[6])) \
        .groupByKey() \
        .map(lambda x: (x[0], set(x[1]))) \
        .mapValues(len) \
        .sortBy(lambda x: x[0]) \
        .collect()
    
    total_computing_time = time.time() - start_computing_time
    print(d)
    print("Computing time: ", str(total_computing_time))


if __name__ == "__main__":
    main(sys.argv[1])
