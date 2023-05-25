import org.apache.spark.sql.types._
import org.apache.spark.sql._


object AppMain {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("SparkLesson2.1")
      .master("local")
      .getOrCreate()

    val restaurantStructType = StructType(
      Seq(StructField("average_cost_for_two", LongType),
      StructField("cuisines", StringType),
      StructField("deeplink", StringType),
      StructField("has_online_delivery", IntegerType),
      StructField("is_delivering_now", IntegerType),
      StructField("menu_url", StringType),
      StructField("name", StringType),
      StructField("opened", StringType),
      StructField("photos_url", StringType),
      StructField("url", StringType),
      StructField("user_rating",
        StructType(Seq(
          StructField("aggregate_rating", StringType),
          StructField("rating_color", StringType),
          StructField("rating_text", StringType),
          StructField("votes", StringType)
        )))
    ))

    val restaurantDataFrame = sparkSession.read
      .format("json")
      .schema(restaurantStructType)
      .load("src/main/resources/restaurant_ex.json")

    restaurantDataFrame.show()

    val restaurantDataFrameMapTypes = restaurantDataFrame.schema.fields.map(f => f.name -> f.dataType).toMap

    val isDeliveringFrameType = restaurantDataFrameMapTypes.get("is_delivering_now").map(_.toString).getOrElse("Not found")
    val hasOnlineDeliveryFrameType = restaurantDataFrameMapTypes.get("has_online_delivery").map(_.toString).getOrElse("Not found")

    println(s"is_delivering_now frame type = $isDeliveringFrameType")
    println(s"has_online_delivery frame type = $hasOnlineDeliveryFrameType")
    sparkSession.stop()
  }
}
