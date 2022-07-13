package airline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, collect_list, lit}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local[*]")
      .getOrCreate()

    var airLineTimeDF = spark.read.parquet("data/airtraffic-part.parquet")

    var airlineDF = spark.sparkContext.textFile("data/airport-codes-na.txt").map(x => x.split("\t"))
    val header = airlineDF.first()
    var rdd2 = airlineDF.filter(x => x(0) != header(0) && x(1) != header(1) && x(2) != header(2) && x(3) != header(3))
      .map(t => Row(t(0),t(1),t(2),t(3)))
    val schema = new StructType()
      .add(StructField("City", StringType, true))
      .add(StructField("State", StringType, true))
      .add(StructField("Country", StringType, true))
      .add(StructField("IATA", StringType, true))

    val dfWithSchema = spark.createDataFrame(rdd2, schema)

    var dfAirpost = dfWithSchema.as("usAirpostDF").join(airLineTimeDF.as("airpostDF"),
      col("usAirpostDF.IATA") === col("airpostDF.Origin"),
    "inner")
      .select(
        col("Country"),
        col("usAirpostDF.IATA").as("airpost"),
        col("Month"),
        col("Year")
      )
      .filter(col("Country") === "USA")
      .filter(col("Month") === 1)
      .filter(col("Year") === 2008)
      .groupBy("airpost").count()

    println(dfAirpost.count())
    dfAirpost.show()

    dfWithSchema.as("usAirpostDF").join(airLineTimeDF.as("airpostDF"),
      col("usAirpostDF.IATA") === col("airpostDF.Origin"),
      "inner")
      .select(
        col("Country"),
        col("state"),
        col("usAirpostDF.IATA").as("airpost"),
        col("Month"),
        col("Year")
      )
      .filter(col("Country") === "USA")
      .filter(col("Month") === 1)
      .filter(col("Year") === 2008)
      .groupBy("state").count()
      .orderBy(col("count").desc).show()

    dfAirpost = dfAirpost.withColumn("Country",lit("USA"))

    val listAirPost = dfAirpost.select(col("airpost"))
      .collect().map(_(0)).toList

    println(dfWithSchema
      .filter(col("Country") === "USA").distinct()
    .count())

    print(dfWithSchema
      .filter(col("Country") === "USA")
      .filter(!col("IATA").isin(listAirPost:_*))
      .count())
  }
}
