package hw2

import java.sql.Timestamp

case class Movie(
                  id: Int,
                  title: String,
                  year: Option[Int],
                  genres: List[String]
                )
case class Rating(userId: Int, movieId: Int, rating: Double, ts: Timestamp)

case class Link(movieId:Int,imdbId:Int,tmdbId:Option[Int])

case class Tag(userId:Int,movieId:Int,tag:String,ts:Timestamp)