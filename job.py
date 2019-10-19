from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


if __name__ == '__main__':
    spark = SparkSession \
            .builder \
            .appName("Simple ETL") \
            .getOrCreate()

    #################################################
    # EXTRACT: LOAD DATA FROM CSV FILE
    #################################################

    data_file = './data/supermarket-sales/supermarket_sales.csv'
    df = spark.read.csv(data_file, header=True, sep=",")
    print("Total row={}".format(df.count()))
    df.printSchema()
    df.show()
    df.describe().show()

    #################################################
    # TRANSFORM: How many transaction and how much total sales value per city?
    #################################################

    df.registerTempTable('sales')
    output = spark.sql('SELECT COUNT(`Invoice ID`) as num_trx, SUM(Total) as total_sales, City from sales GROUP BY City')
    output.show()

    #################################################
    # LOAD: save result to file
    #################################################

    output.write.format('json').save('summary.json')