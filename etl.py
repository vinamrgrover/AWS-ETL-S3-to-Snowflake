from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import when
import re

spark = SparkSession.builder \
                    .appName('S3_to_Snowflake') \
                    .getOrCreate()


def remove_digit(string : str) -> [str, None]:
    if string:
        try:
            tmp_str = re.search(r'(\w+(\.[A-Za-z0-9]+)+)', string)
        except AttributeError:
            tmp_str = re.search(r'\w+', string)

        if tmp_str:
            return tmp_str.group()
        else:
            return None

    else:
        return None


remove_digit_func = udf(lambda x : remove_digit(x))


def rename_cols(df, mapping : dict):  # EMR-Serverless doesn't support `df.withColumnsRenamed()`
    for old, new in mapping.items():
        df = df.withColumnRenamed(old, new)

    return df


def transform(df):
    old_cols = df.columns
    new_cols = ['_'.join(x.split()).lower() for x in df.columns]

    df = rename_cols(df, dict(zip(old_cols, new_cols)))

    df = df.withColumns({x : remove_digit_func(col(x)) for x in df.columns[1:6]})

    df = df.replace('NO', 'Partially Covered') \
           .replace('393', 'Partially Covered') \
           .replace('YES', 'Fully Covered') \
           .fillna('Partially Covered') \
           .filter(~ df.state_name.contains('"'))

    df = df.withColumns(
        {
            'sc_current_population' : df.sc_current_population.cast(IntegerType()),
            'st_current_population' : df.st_current_population.cast(IntegerType()),
            'general_current_population' : df.st_current_population.cast(IntegerType()),
            'sc_covered_population' : df.sc_covered_population.cast(IntegerType()),
            'st_covered_population' : df.st_covered_population.cast(IntegerType()),
            'general_covered_population' : df.general_covered_population.cast(IntegerType())
        }
    )

    df = df.drop('year', 'sc_concentrated', 'st_concentrated')

    df = df.withColumns(
        {x : when(col(x).isin(['Partially Covered', 'Fully Covered']), None).otherwise(col(x)) for x in df.columns[1:6]}
    )

    return df


def main():
    df = spark.read.csv('s3://emr-lake/raw/habitation.csv', header = True)

    df = transform(df)

    df.write.format('parquet') \
            .option('compression', 'snappy') \
            .partitionBy('state_name') \
            .save('s3://emr-lake/transformed/habitation/')


if __name__ == '__main__':
    main()
