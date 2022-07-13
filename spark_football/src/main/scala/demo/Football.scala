package demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lit, round, row_number, sum, when}

object Football {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Demo")
      .master("local[*]")
      .getOrCreate()

    var matchesDF = spark.read
      .format("csv")
      .option("header", "true")
      .load("data/matches.csv")
      .filter(col("Season") >= 2000 && col("Season") <= 2010)
      .filter(col("Div") === "D1")

    matchesDF = matchesDF
      .withColumn("HomeWin", when(col("FTHG") > col("FTAG"), 1).otherwise(0))
      .withColumn("HomeLoss", when(col("FTHG") < col("FTAG"), 1).otherwise(0))
      .withColumn("Tie", when(col("FTHG") === col("FTAG"), 1).otherwise(0))

    var home_goal = matchesDF.groupBy("Season", "HomeTeam")
      .agg(
        sum("FTHG").as("FTHG_Home"),
        sum("FTAG").as("FTAG_Home"),
        sum("HomeWin").as("HomeWin_Home"),
        sum("HomeLoss").as("HomeLoss_Home"),
        sum("Tie").as("Tie_Home")
      )

    var away_goal = matchesDF.groupBy("Season", "AwayTeam")
      .agg(
        sum("FTHG").as("FTHG_Away")
        , sum("FTAG").as("FTAG_Away")
        , sum("HomeWin").as("HomeWin_Away")
        , sum("HomeLoss").as("HomeLoss_Away")
        , sum("Tie").as("Tie_Away")
      )

    var result = home_goal.as("home").join(
      away_goal.as("away")
      , col("home.HomeTeam") === col("away.AwayTeam") && col("home.Season") === col("away.Season")
      , "inner"
    )
//      .withColumn("Team",col("home.HomeTeam"))
//      .withColumn("GoalsScored",col("FTHG_Home") + col("FTAG_Away"))
//      .withColumn("GoalsAgainst",col("FTAG_Home") +  col("FTHG_Away"))
//      .withColumn("GoalDifferentials",col("GoalsScored") - col("GoalsAgainst"))
//      .withColumn("Win",col("HomeWin_Home") + col("HomeLoss_Away"))
//      .withColumn("Loss",col("HomeLoss_Home") + col("HomeWin_Away"))
//      .withColumn("Tie",col("Tie_Home") + col("Tie_Away"))
      .withColumn("Team",when(col("home.HomeTeam") =!= null,col("home.HomeTeam"))
        .otherwise(col("away.AwayTeam")))
      .withColumn("GoalsScored",when(col("FTAG_Away") === null,col("FTHG_Home"))
        .when(col("FTHG_Home") === null,col("FTAG_Away"))
        .otherwise(col("FTHG_Home") + col("FTAG_Away")))
      .withColumn("GoalsAgainst",when(col("FTHG_Away") === null,col("FTAG_Home"))
        .when(col("FTAG_Home") === null,col("FTHG_Away"))
        .otherwise(col("FTAG_Home") +  col("FTHG_Away")))
      .withColumn("GoalDifferentials",col("GoalsScored") - col("GoalsAgainst"))
      .withColumn("Win",when(col("HomeLoss_Away") === null,col("HomeWin_Home"))
        .when(col("HomeWin_Home") === null,col("HomeLoss_Away"))
        .otherwise(col("HomeWin_Home") + col("HomeLoss_Away")))
      .withColumn("Loss", when(col("HomeWin_Away") === null,col("HomeLoss_Home"))
        .when(col("HomeLoss_Home") === null,col("HomeWin_Away"))
        .otherwise(col("HomeLoss_Home") + col("HomeWin_Away")))
      .withColumn("Tie", when(col("Tie_Away") === null,col("Tie_Home"))
        .when(col("Tie_Home") === null,col("Tie_Away"))
        .otherwise(col("Tie_Home") + col("Tie_Away")))
      .withColumn("WinPct", round(lit(100) * lit(col("Win") / lit(col("Win") + col("Loss") + col("Tie"))),2))
      .select(
        col("Team"),
        col("home.Season").as("Season"),
        col("GoalsScored"),
        col("GoalsAgainst"),
        col("GoalDifferentials"),
        col("Win"),
        col("Loss"),
        col("Tie"),
        col("WinPct")
      )

    val windowSpec = Window.partitionBy("Season").orderBy(col("WinPct").desc)
    result = result.withColumn("row_number",row_number.over(windowSpec))
      .filter(col("row_number") === 1)

    result
      .orderBy(col("Season"))
      .show()
  }
}
