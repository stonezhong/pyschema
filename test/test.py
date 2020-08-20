#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from pyspark.sql import SparkSession

from pyschema2 import JSONConverter, py_type_from_json_schema, array_of, nullable, \
    STRING_TYPE, LONG_TYPE, BOOLEAN_TYPE, TIMESTAMP_TYPE, print_pyschema


def get_spark():
    # spark = SparkSession.builder.\
    #     config("spark.executor.memory", "2g").\
    #     config("spark.driver.memory", "2g").\
    #     appName(f"RunJob-{os.getpid()}").getOrCreate()
    spark = SparkSession.builder.appName("pyspark-unit-test").getOrCreate()
    return spark

CORD_TYPE = nullable({
    "type": "object",
    "properties": {
        "x": LONG_TYPE,
        "y": LONG_TYPE,
    }
})

POLYGON_TYPE = nullable({
    "type": "object",
    "properties": {
        "receivedAt": TIMESTAMP_TYPE,
        "color":      STRING_TYPE,
        "cords": array_of(CORD_TYPE)
    }
})

def app_main(spark):
    schema = POLYGON_TYPE
    pyschema, _ = py_type_from_json_schema(schema)
    print("**********************")
    print_pyschema(pyschema)
    print("**********************")
    # rows = [
    #     {"x": 1, "y": 2},
    #     {"x": 3, "y": 4},
    # ]

    rows = [
        {"receivedAt": "2020-08-19T20:19:58.261Z", "color": "red",   "cords": [{"y": 2}]},
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
