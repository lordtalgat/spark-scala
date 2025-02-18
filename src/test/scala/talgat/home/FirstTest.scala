package talgat.home

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType}
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date

class FirstTest extends AnyFunSuite {

  private val spark = SparkSession.builder()
    .appName("FirstTest")
    .master("local[*]")
    .getOrCreate()

  private val schema = StructType(Seq(
    StructField("date", DateType, nullable = true),
    StructField("open", DoubleType, nullable = true),
    StructField("close", DoubleType, nullable = true),
    StructField("rank", IntegerType, nullable = true)
  ))

  test("First unit test ") {
    val testRows = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0, 1),
      Row(Date.valueOf("2023-03-01"), 1.0, 2.0, 1),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0, 1)
    )

    val expectedRows = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0, 1),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0, 1)
    )
    implicit val encoder: Encoder[Row] = Encoders.row(schema)
    val testData = spark.createDataset(testRows)
    val res = Main.higestClosingPricePerYear(testData).collect()

    res should contain theSameElementsAs expectedRows
  }
}
