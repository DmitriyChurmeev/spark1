import org.apache.spark.sql.{Row, SparkSession}

object AppMain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkLesson1")
      .master("local")
      .getOrCreate()

    import spark.implicits._
//    val coursesDF = courses.toDF("title", "duration (h)")

//    coursesDF.show()
//    coursesDF.printSchema()
    import org.apache.spark.sql.types._
    val data = Seq(
      Row("s9FH4rDMvds", "2020-08-11T22:21:49Z", "UCGfBwrCoi9ZJjKiUK8MmJNw", "2020-08-12T00:00:00Z"),
      Row("kZxn-0uoqV8", "2020-08-11T14:00:21Z", "UCGFNp4Pialo9wjT9Bo8wECA", "2020-08-12T00:00:00Z"),
      Row("QHpU9xLX3nU", "2020-08-10T16:32:12Z", "UCAuvouPCYSOufWtv8qbe6wA", "2020-08-12T00:00:00Z")
    )

    val schema = Array(
      StructField("videoId", StringType),
      StructField("publishedAt", StringType),
      StructField("channelId", StringType),
      StructField("trendingDate", StringType)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    df.show()

    spark.stop()
  }



}
