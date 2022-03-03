# This is a the coding challange script.
from service.mongo_service import MongoService as ms
from service.csv_service import CsvService as cs
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, avg
import matplotlib.pyplot as plt
from itertools import repeat
import numpy as np
import logging
import sys


def set_logger():
    """ Setup logger for the application"""
    logger = logging.getLogger("my logger")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)

    file_handler = logging.FileHandler('logs/logs.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
    return logger


def print_hist(x, y, nation, as_grid=True):
    x_pos = []
    for index in range(0, len(x)):
        x_pos.append(index * 1)
    width = list(repeat(1, len(x)))
    # Create names on the x-axis
    plt.xticks(x_pos, x)
    plt.xticks(rotation=90, fontsize=8)
    # Create names on the y-axis
    y_pos = np.arange(len(x))
    plt.bar(y_pos, y, color="red", width=width)
    # details
    plt.title(f'Product distribution for {nation}')
    plt.ylabel('Quantity')
    plt.xlabel('InvoiceNo')
    plt.grid(as_grid)
    # store img in img/ folder
    plt.savefig(f'out/img/dist_prod_{nation}.png')
    plt.close()


def print_line(df, x_axes, y_axes, axs, row_idx, col_idx):
    """ print distribution over x with line graph """
    d1 = df.groupBy(x_axes).agg(avg(y_axes).alias("avg_price")).toPandas()
    # 1.set_index(x_axes)['avg_price'].plot(kind="line", color='red')
    axs[row_idx, col_idx].plot(d1[x_axes], d1["avg_price"])
    axs[row_idx, col_idx].set(xlabel=x_axes, ylabel='avg_price')
    axs[row_idx, col_idx].set_xticklabels(axs[row_idx, col_idx].get_xticks(), rotation=90)
    axs[row_idx, col_idx].set_title(f'Distribution -> avg({y_axes})/num({x_axes})')
    # plt.subplot(2, 3, index)


def get_analytics(db_name, coll_name, db_url='127.0.0.1'):
    logger = logging.getLogger("my logger")
    if db_name is None:
        logger.error("[Error] DB name is null.")
        return
    if coll_name is None:
        logger.error("[Error] Collection name is null.")
        return
    if db_url is None:
        logger.error("[Error] Url is null.")
    connection_string = "mongodb://127.0.0.1/" + db_name + "." + coll_name
    print("Establishing Spark connection to path [%s]" % connection_string)

    spark = SparkSession.builder \
        .appName("myApp") \
        .master("local") \
        .config("spark.mongodb.input.uri", connection_string) \
        .config("spark.mongodb.output.uri", connection_string) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
        .getOrCreate()

    df = spark.read.format("mongo").load().dropna()
    # printing head() schema for test
    df.show(truncate=True)
    df.printSchema()

    # which product sold most?
    q1 = df.groupBy("InvoiceNo") \
        .agg(sum("Quantity").alias("qty_per_invoice")) \
        .sort("qty_per_invoice", ascending=False)
    logger.info("a1 - which product sold most?: {}".format(q1.head()))

    # which customer spent the most
    q2 = df \
        .withColumn('tot_price', (col('Quantity') * col('UnitPrice'))) \
        .groupBy("CustomerID") \
        .agg(sum("tot_price").alias("tot_by_cust")) \
        .sort("tot_by_cust", ascending=False)
    logger.info("a2 - which customer spent the most?: {}".format(q2.head()))

    # ratio between price and quantity for each invoice - avg ratio per invoice (seems suitable to do)
    q3 = df \
        .withColumn('partial_ratio', (col('UnitPrice') / col('Quantity'))) \
        .groupBy("InvoiceNo") \
        .agg(avg("partial_ratio").alias("avg_ratio")) \
        .sort("avg_ratio", ascending=False)
    logger.info("a3 - ratio between price and quantity for each invoice?: result in out/ratio_price_qt.csv.")
    q3.toPandas().to_csv('out/ratio_price_qt.csv', encoding='utf-8')

    # matplotlib inline - setup figure
    plt.rcParams.update({'figure.figsize': (22, 15), 'figure.dpi': 80})

    # graph of the distribution of each product for each of the available countries
    nations = list(df.select(col("Country")).distinct().collect())
    # printing the in of each nation
    q4 = df.groupBy("Country", "InvoiceNo") \
        .agg(sum("Quantity").alias("qty_for_invAndNation"))
    for i, nation in enumerate(nations):
        logger.info("%d. selected nation: %s" % (i, str(nation)))
        data = q4.select(col('InvoiceNo'), col('qty_for_invAndNation')) \
            .filter(q4.Country == f"{nation.Country}") \
            .toPandas()
        x = list(data['InvoiceNo'])
        y = list(data['qty_for_invAndNation'])
        print_hist(x, y, nation)

    # graph of the distribution of price
    fig, axs = plt.subplots(3, 2)
    # avg(price)/num(invoice)
    print_line(df=df, x_axes="InvoiceNo", y_axes="UnitPrice", axs=axs, row_idx=0, col_idx=0)
    logger.info("Creating image Price distribution [InvoiceNo/UnitPrice]")
    # avg(price)/StockCode
    print_line(df=df, x_axes="StockCode", y_axes="UnitPrice", axs=axs, row_idx=0, col_idx=1)
    logger.info("Creating image Price distribution [StockCode/UnitPrice]")
    # avg(price)/InvoiceDate
    print_line(df=df, x_axes="InvoiceDate", y_axes="UnitPrice", axs=axs, row_idx=1, col_idx=0)
    logger.info("Creating image Price distribution [InvoiceDate/UnitPrice]")
    # avg(price)/CustomerID
    print_line(df=df, x_axes="CustomerID", y_axes="UnitPrice", axs=axs, row_idx=1, col_idx=1)
    logger.info("Creating image Price distribution [CustomerID/UnitPrice]")
    # avg(price)/Country
    print_line(df=df, x_axes="Country", y_axes="UnitPrice", axs=axs, row_idx=2, col_idx=0)
    logger.info("Creating image of distribution [Country/UnitPrice]")
    # then store the image
    logger.info("Saving image into out/img/distribute_price")
    fig.savefig('out/img/distribute_price.png')
    plt.close()


def main() -> int:
    """
    `cli_args` makes it possible to call this function command-line-style
    from other Python code without touching sys.argv.
    """
    set_logger()
    logger = logging.getLogger("my logger")
    try:
        retails = cs.open_excel("in/online_retail.xlsx")
        if retails is not None:
            logger.info("File found. Start import of the file.")
            collection = ms.mongo_connect("test", "online_retail", "localhost", 27017)
            if collection is not None:
                ms.mongo_import(collection, retails)
                get_analytics("test", "online_retail", "localhost")
                print("Executed.")
                return 0  # success
        return 1

    except KeyboardInterrupt:
        print('Aborted manually.', file=sys.stderr)
        return 1

    except Exception as err:
        logger.error("Uncaught Error has been found. Caused by: %s" % err)
        # (in real code the `except` would probably be less broad)
        # Turn exceptions into appropriate logs and/or console output.
        return 1


# __main__ support is still here to make this file executable without
# installing the package first.
if __name__ == '__main__':
    sys.exit(main())
