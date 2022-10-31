// spark imports
import org.apache.spark._
import org.apache.spark.sql._

import scala3encoders.derivation.{Deserializer, Serializer}
import org.apache.spark.sql.catalyst.DeserializerBuildHelper.createDeserializerForTypesSupportValueOf
import org.apache.spark.sql.catalyst.DeserializerBuildHelper.createDeserializerForScalaBigInt
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, LongType, DecimalType}
import scala3encoders.encoder

given Deserializer[BigInt] with
  def inputType: DataType = LongType
  def deserialize(path: Expression): Expression =
    createDeserializerForScalaBigInt(path)

given Serializer[BigInt] with
  def inputType: DataType = LongType
  def serialize(inputObject: Expression): Expression = inputObject

object spark1 {

  def main(args: Array[String]) : Unit = {
    // create spark session
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("SparkTestApp")
      .getOrCreate()
    import spark.sqlContext.implicits._
    val dataFolder = "../Spark-The-Definitive-Guide"

    case class Flight(DEST_COUNTRY_NAME: String,
                      ORIGIN_COUNTRY_NAME: String,
                      count: BigInt)
    val flightsDF = spark.read
      .parquet(dataFolder + "/data/flight-data/parquet/2010-summary.parquet/")

    val flights = flightsDF.as[Flight]
    flights
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .take(5)
  }
}
