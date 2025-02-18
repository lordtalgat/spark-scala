package talgat.home

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark-scala")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")

    //persist to cache
    val pdf = df.persist()
    pdf.printSchema()

    //rename data frame to correct way
    val renamedDf = pdf.select(renameColumns(pdf.columns): _*)


    val stockData1 = renamedDf
      .withColumn("diff", col("close") - col("open"))
      .filter(col("close") > col("open") * 1.1)
      .count()

    println("____________________________________________________________________")
    println(s" difference open and close more than 1.1 count = $stockData1")
    println("____________________________________________________________________")

    // max close date and year
    higestClosingPricePerYear(renamedDf).show()

    // Show max close and avg close
    show50MaxCloseAndAvgClose(renamedDf).show(50)
  }

  def renameColumns(columns: Array[String]): Array[Column] = {
    columns.map { c =>
      if (c.contains(" ")) {
        col(c).as(c.split(" ").foldLeft("", false) { case ((str, b), el) =>
          if (!b) (el.toLowerCase, true)
          else (str + el, true)
        }._1)
      } else {
        col(c).as(c.toLowerCase())
      }
    }
  }


  def higestClosingPricePerYear(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val window = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)

    df
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .sort($"close".desc)
  }

  def show50MaxCloseAndAvgClose(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    df
      .groupBy(year($"date").as("year"))
      .agg(max($"close").as("maxClose"), avg($"close").as("avgClose"))
      .sort($"maxClose".desc)
  }
}
