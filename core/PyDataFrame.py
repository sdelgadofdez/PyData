from pyspark.sql import SparkSession
import pyspark.sql.functions as func

import sys
import time


def main(file_name: str) -> None:
    spark_session = SparkSession \
        .builder \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    start_computing_time = time.time()

    data_frame = spark_session \
        .read \
        .format("csv") \
        .options(header='true', inferschema='true') \
        .load(file_name)

    data_frame.printSchema()
    data_frame.show()

    data_frame \
        .groupBy("Topic") \
        .agg(func.countDistinct("Question")) \
        .sort("Topic") \
        .show()

    total_computing_time = time.time() - start_computing_time
    print("Computing time: ", str(total_computing_time))


if __name__ == '__main__':
    main(sys.argv[1])
