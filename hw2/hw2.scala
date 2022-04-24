package hw2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType,IntegerType}

object hw2 {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().master("local[*]").appName("IntroRDD").getOrCreate()
    import spark.sqlContext.implicits._
    val sc = spark.sparkContext

    val movies = sc.textFile(s"${Parsing.dPath}/movies.csv").filter(l => l!="movieId,title,genres").map(Parsing.parseMovie)
    val ratings = sc.textFile(s"${Parsing.dPath}/ratings.csv").filter(_!="userId,movieId,rating,timestamp").map(Parsing.parseRating)
    val tags = sc.textFile(s"${Parsing.dPath}/tags.csv").filter(_!="userId,movieId,tag,timestamp").map(Parsing.parseTag)
    val links = sc.textFile(s"${Parsing.dPath}/links.csv").filter(_!="movieId,imdbId,tmdbId").map(Parsing.parseLink)

    val moviesDF = movies.toDF()
    val ratingsDF = ratings.toDF()
    val tagsDF = tags.toDF()
    val linksDF = links.toDF()

    //Question1:

    /*
    moviesDF.select(col("id"),col("year"),$"id",expr("movieId")).show(2)

    moviesDF.withColumn("myNumovies.mber",lit("dsadsa")).show(2)
     */

    val explodedMovie = moviesDF.select($"year",explode($"genres").as("genres"))
                                .groupBy("year","genres")
                                .agg(count($"year").as("number_of_film"))

    println(explodedMovie.orderBy(col("number_of_film").desc).show(20))

    //Question2:

    val joinedMovRat = ratingsDF.join(moviesDF,ratingsDF("movieId") === moviesDF("Id"),"inner")
                                .select($"movieId",$"year",$"rating")
                                .filter($"rating"===5.0)

    val groupedMovRat = joinedMovRat.groupBy("year")
                                    .agg(count($"movieId").as("number_of_film"))

    println(groupedMovRat.orderBy(col("number_of_film").desc).show(20))

    //Question 3: (toplam değeri getirmenin yoluna bak!)

    val explodedMovies = moviesDF.select($"Id",$"year",explode($"genres").as("genres"))
                                 .withColumn("total_number_of_films",lit(9742))
                                 .groupBy("year","genres","total_number_of_films")
                                 .agg(countDistinct($"Id").as("number_of_distinct_film"))
                                 .withColumn("ratio",lit($"number_of_distinct_film"/$"total_number_of_films"))
                                 .orderBy("number_of_distinct_film")

    println(explodedMovies.show(50))


    // Question 4:

    val joinedTagRat = tagsDF.join(ratingsDF,tagsDF("movieId") === ratingsDF("movieId") && tagsDF("userId") === ratingsDF("userId"),"left")
      .drop(ratingsDF.col("movieId")).drop(ratingsDF.col("userId"))
        .select($"userId",$"movieId",$"tag",$"rating")

    val joinedTagRatMov = joinedTagRat.join(moviesDF,joinedTagRat("movieId") === moviesDF("Id"),"inner" )
      .drop(moviesDF.col("Id")).drop(moviesDF.col("title"))
      .select($"userId",$"movieId",$"tag",$"rating",$"year",explode($"genres").as("genres"))

    val temp1 = joinedTagRatMov.groupBy("year","genres").agg(countDistinct($"userId").as("number_of_people_tagged"))

    val temp2 = joinedTagRatMov.filter($"rating".isNotNull)
                               .groupBy("year","genres")
                               .agg(countDistinct($"userId").as("number_of_people_rated"))

      val result = temp1.join(temp2, temp1("year")===temp2("year") &&  temp1("genres")===temp2("genres"),"inner" )
        .drop(temp2.col("year"))
        .drop(temp2.col("genres"))
        .withColumn("ratio",$"number_of_people_rated"/$"number_of_people_tagged")
        .orderBy($"ratio".desc)

    result.show(false)

    // Question 5:

    val mostTagged = tagsDF.groupBy("userId").agg(count($"tag").as("number_of_tag")).orderBy($"number_of_tag".desc).limit(1)

    println(s"The most tagged person is ${mostTagged.first.get(0).toString} with  ${mostTagged.first.get(1).toString} tags!")

    val ratingswithMovies = ratingsDF.join(moviesDF, ratingsDF("movieId") === moviesDF("Id"), "inner")
      .drop(moviesDF.col("Id")).drop(ratingsDF.col("ts"))
      .filter($"userId" === mostTagged.first.get(0))

    val mostLiked = ratingswithMovies.select($"year",$"rating",explode($"genres").as("genres")).groupBy("year","genres")
      .agg(mean($"rating").as("avg_of_ratio"))
      .orderBy($"avg_of_ratio".desc)

    mostLiked.show(false)

    val mostDisliked = ratingswithMovies.select($"year",$"rating",explode($"genres").as("genres")).groupBy("year","genres")
      .agg(mean($"rating").as("avg_of_ratio"))
      .orderBy($"avg_of_ratio".asc)

    mostDisliked.show(false)

//Question 6:

    val joinedTagRat2 = tagsDF.join(ratingsDF,tagsDF("movieId") === ratingsDF("movieId") && tagsDF("userId") === ratingsDF("userId"),"inner")
      .drop(ratingsDF.col("movieId")).drop(ratingsDF.col("userId"))
      .select($"userId",$"movieId",$"tag",tagsDF.col("ts").as("tag_ts"),$"rating",ratingsDF.col("ts").as("rating_ts"))

    val joinedTagRatMov2 = joinedTagRat2.join(moviesDF,joinedTagRat("movieId") === moviesDF("Id"),"inner" )
      .drop(moviesDF.col("Id")).drop(moviesDF.col("title"))
      .select($"userId",$"movieId",$"tag",$"tag_ts",$"rating",$"rating_ts",explode($"genres").as("genres"))

    val resultTable = joinedTagRatMov2.withColumn("check", when(col("tag_ts") < col("rating_ts"), "tag").otherwise("rating"))

    val aggResultTable = resultTable.groupBy("genres","check").agg(countDistinct($"movieId").as("first_event_number")).orderBy($"first_event_number".desc)

    aggResultTable.show(false)

  }
}
