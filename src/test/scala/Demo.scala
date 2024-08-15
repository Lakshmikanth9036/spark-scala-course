import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object Demo {

  case class Movie(userId: Int, movieName: String, releaseDate: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PopularMovie")
      .master("local[*]")
      .getOrCreate();

    val movieSchema = new StructType()
      .add("userId", IntegerType, nullable = true)
      .add("movieId", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    // Load up movie data as dataset
    val moviesDS = spark.read
      .option("sep", "\t")
      .schema(movieSchema)
      .csv("data/ml-100k/u.data")
      .as[Movie]

    // Some SQL-style magic to sort all movies by popularity in one line!
    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count"))

    // Grab the top 10
    topMovieIDs.show(10)

    // Stop the session
    spark.stop()
  }
}
