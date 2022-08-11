from pyspark.sql import SparkSession, functions as f
from pyspark.sql.window import Window

from const import (
    columns as c,
    PaymentTypes,
    TAXI_DATA_PATH,
    RESULT_PATH,
    VendorTypes,
    NA,
)


def read_input_file(spark, path):
    return spark.read.csv(path, header=True)


def map_df_values(taxi_df):
    # create a new 'Vendor' column based on the original 'Vendor_ID', change the values in the "Vendor" column
    taxi_df = taxi_df.withColumn(
        c.vendor,
        f.when(f.col(c.vendor_id) == "1", VendorTypes.creative_mob_tech)
        .when(f.col(c.vendor_id) == "2", VendorTypes.verifone)
        .otherwise(NA),
    )

    # create a new 'Payment Type' column based on the original 'payment_type',
    # change the values in the "Payment Type" column
    taxi_df = taxi_df.withColumn(
        c.payment_type,
        f.when(f.col(c.payment_type_initial) == "1", PaymentTypes.credit_card)
        .when(f.col(c.payment_type_initial) == "2", PaymentTypes.cash)
        .when(f.col(c.payment_type_initial) == "3", PaymentTypes.no_charge)
        .when(f.col(c.payment_type_initial) == "4", PaymentTypes.dispute)
        .when(f.col(c.payment_type_initial) == "5", PaymentTypes.unknown)
        .when(f.col(c.payment_type_initial) == "6", PaymentTypes.voided_trip)
        .otherwise(NA),
    )

    return taxi_df


def add_stats_to_df(mapped_df):
    # sum of values 'total_amount' column and 'passenger_count' column
    df = mapped_df.groupBy(c.vendor, c.payment_type).agg(
        f.sum(c.total_amount).alias(c.ta_per_vendor),
        f.sum(c.passenger_count).alias(c.pc_per_vendor),
    )

    # create a new 'Payment Rate' column with next values: 'ta_per_vendor'/'pc_per_vendor'
    df = df.withColumn(c.payment_rate, f.col(c.ta_per_vendor) / f.col(c.pc_per_vendor))

    # Window function. Create a new 'Next Payment Rate' column with next values:
    # next record(bigger) payment rate for vendor
    w = Window.partitionBy(c.vendor).orderBy(f.desc(c.payment_rate))
    df = df.withColumn(c.next_payment_rate, f.lag(c.payment_rate).over(w))

    # Window function. Create a new 'Max Payment Rate' column (max payment rate for vendor)
    w = Window.partitionBy(c.vendor)
    df = df.withColumn(c.max_payment_rate, f.max(c.payment_rate).over(w))

    # Create a new 'Percents to next Rate' column, which describes how many points is necessary
    # to achieve the next record payment rate
    df = df.withColumn(
        c.percents_to_next_payment_rate,
        f.concat(
            f.round(f.col(c.payment_rate) / f.col(c.next_payment_rate) * 100, 2),
            f.lit(" %"),
        ),
    )

    # delete unnecessary columns: 'ta_per_vendor' and 'pc_per_vendor'
    return df.drop(c.ta_per_vendor, c.pc_per_vendor).orderBy(c.vendor, c.payment_rate)


if __name__ == "__main__":
    # entrance point
    spark = (
        SparkSession.builder.master("local")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    )

    # read csv file
    df = read_input_file(spark, TAXI_DATA_PATH)

    mapped_df = map_df_values(taxi_df=df)
    df = add_stats_to_df(mapped_df)

    # save as csv file
    df.coalesce(1).write.csv(path=RESULT_PATH, header=True, mode="overwrite")
