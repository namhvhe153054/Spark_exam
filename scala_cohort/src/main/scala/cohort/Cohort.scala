package cohort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, datediff, dayofmonth, explode, expr, min, month, posexplode, soundex, split, sum, to_date, udf, when, year}
import org.apache.spark.sql.types.{ArrayType, DateType, StringType, StructField, StructType}

import java.sql.Date
import scala.collection.mutable.ListBuffer

object Cohort {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local[*]")
      .getOrCreate()

    var cohortDF = spark.read
      .option("header", "True")
      .csv("data/online_retail.csv")

    cohortDF = cohortDF.withColumn("InvoiceDateString", col("InvoiceDate").cast(StringType))
      .withColumn("date", to_date(split(col("InvoiceDateString"), " ")(0), "M/d/yyyy"))

    var dateDF = cohortDF.select(col("date")).distinct()
      .filter(year(col("date")) === 2010)
      .filter(month(col("date")) === 12)
      .filter(dayofmonth(col("date")) >= 1 && dayofmonth(col("date")) <= 10)
      .orderBy("date")

    val dateStart = dateDF.first().getDate(0)

    val dateEnd = dateDF.orderBy(col("date").desc).first().getDate(0)

    val dateSeq = List(Row(dateStart,dateEnd,getDateList(dateStart,dateEnd)))

    val schema = StructType(Array(
      StructField("dateStart", DateType, true),
      StructField("dateEnd", DateType, true),
      StructField("diff",ArrayType(DateType),true)
    ))

    import spark.implicits._

    var df = spark.createDataFrame(spark.sparkContext.parallelize(dateSeq), schema)
    df = df.withColumn("date",explode(col("diff")))
      .drop("dateStart","dateEnd","diff")

    dateDF = dateDF.as("date1DF").join(df.as("date2DF"))
      .select(
        col("date1DF.date").as("date1"),
        col("date2DF.date").as("date2")
      )
      .orderBy("date1", "date2")

    var minDateCustomer = cohortDF.groupBy("CustomerID")
      .agg(min(col("date")).as("min"))

    cohortDF = cohortDF.as("cohort").join(minDateCustomer.as("minDate"),
      col("cohort.CustomerID") === col("minDate.CustomerID"),
      "left")
      .select(
        col("InvoiceNo"),
        col("cohort.CustomerID"),
        col("date").as("dateInvoice"),
        col("min").as("dateMin")
      )

    dateDF = dateDF.as("dateDF").join(cohortDF.as("cohortDF"),
      col("dateDF.date1") === col("cohortDF.dateMin")
        && col("dateDF.date2") === col("cohortDF.dateInvoice"),
      "left"
    ).select(
      col("dateDF.date1").as("date"),
      col("dateDF.date2").as("dateInvoice"),
      col("cohortDF.CustomerID"),
      col("cohortDF.InvoiceNo")
    ).withColumn("count", when(col("InvoiceNo").isNull, 0)
      .otherwise(1))
      .groupBy("date", "dateInvoice")
      .agg(
        sum(col("count")).as("countInvoice")
      )
      .orderBy("date", "dateInvoice")
      .filter(col("dateInvoice") >= col("date"))

    dateDF = dateDF.groupBy("date")
      .agg(
        collect_list("dateInvoice").as("dateInvoice"),
        collect_list("countInvoice").as("countInvoice")
      ).orderBy("date")
  }

  def getDateList(dateStart: Date,dateFrom: Date): List[Date] ={
    var dates = ListBuffer[Date]()
    var date = dateStart.toLocalDate
    dates += Date.valueOf(date)
    while(date.isBefore(dateFrom.toLocalDate)){
      date = date.plusDays(1)
      dates += Date.valueOf(date)
    }
    dates.toList
  }
}
