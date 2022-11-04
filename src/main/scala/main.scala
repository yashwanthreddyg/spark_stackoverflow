// spark imports
import org.apache.spark.*
import org.apache.spark.sql.*
import scala3encoders.derivation.{Deserializer, Serializer}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.types.{DataType, LongType, ObjectType}

given Deserializer[BigInt] with
  def inputType: DataType = LongType
  def deserialize(path: Expression): Expression =
    StaticInvoke(
      BigInt.getClass,
      ObjectType(classOf[BigInt]),
      "apply",
      path :: Nil,
      returnNullable = false
    )

given Serializer[BigInt] with
  def inputType: DataType = ObjectType(classOf[BigInt])
  def serialize(inputObject: Expression): Expression =
    Invoke(inputObject, "longValue", LongType, returnNullable = false)

object spark1 {

  def main(args: Array[String]) : Unit = {
    // create spark session
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("SparkTestApp")
      .getOrCreate()
    import scala3encoders.given
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
