import sys

from pyspark import SparkConf, SparkContext
import time


def main(file_name: str) -> None:
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    start_computing_time = time.time()

    data = spark_context \
        .textFile(file_name) \
        .map(lambda line: line.split(",")) \
        .map(lambda list: (list[5], 1)) \
        .reduceByKey(lambda x, y: x + y)

    result = data.collect()

    for pair in result:
        print(pair)

    total_computing_time = time.time() - start_computing_time
    print("Computing time: ", str(total_computing_time))

    spark_context.stop()


if __name__ == "__main__":

    main("U.S._Chronic_Disease_Indicators__CDI_.csv")
