#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from pyspark.sql import SparkSession

from pyschema2 import JSONConverter, py_type_from_json_schema, array_of, \
    STRING_TYPE, INTEGER_TYPE, BOOLEAN_TYPE, \
    NULLABLE_STRING_TYPE, NULLABLE_INTEGER_TYPE, NULLABLE_BOOLEAN_TYPE, \
    TIMESTAMP_TYPE, NULLABLE_TIMESTAMP_TYPE


def get_spark():
    # spark = SparkSession.builder.\
    #     config("spark.executor.memory", "2g").\
    #     config("spark.driver.memory", "2g").\
    #     appName(f"RunJob-{os.getpid()}").getOrCreate()
    spark = SparkSession.builder.appName(f"pyspark-unit-test").getOrCreate()
    return spark

CORD_TYPE = {
    "type": "object",
    "properties": {
        "x": NULLABLE_INTEGER_TYPE,
        "y": NULLABLE_INTEGER_TYPE,
    }
}

POLYGON_TYPE = {
    "type": "object",
    "properties": {
        "receivedAt": NULLABLE_TIMESTAMP_TYPE,
        "color": NULLABLE_STRING_TYPE,
        "cords": array_of(CORD_TYPE, nullable=True)
    }

}

def app_main(spark):
    schema = POLYGON_TYPE
    pyschema, _ = py_type_from_json_schema(schema)
    # print(pyschema)

    # rows = [
    #     {"x": 1, "y": 2},
    #     {"x": 3, "y": 4},
    # ]

    rows = [
        {"receivedAt": "2020-08-19T20:19:58.261Z", "color": "red",   "cords": [{"x": 1, "y": 2}]},
        {"receivedAt": "2020-08-19T20:19:58.261Z", "color": "green", "cords": [{"x": 3, "y": 4}]},
        {"receivedAt": "2020-08-19T20:19:58.261Z", "color": "green", "id": 1},
    ]

    # rows = [
    #     {"color": "red"},
    #     {"color": "green"},
    #     {"color": "green"},
    # ]

    parquet_filename = "/home/stonezhong/temp/foo.parquet"

    jcvt = JSONConverter()
    jcvt.json_rows_to_parquet(spark, rows, pyschema, parquet_filename)


    # load the file
    print("after loading")
    df = spark.read.parquet(parquet_filename)
    for row in df.collect():
        print(row)
    df.printSchema()
    df.show(truncate=False)


def main():
    spark = get_spark()

    try:
        app_main(spark)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
