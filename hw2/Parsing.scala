package hw2

import java.sql.Timestamp
import scala.util.Try

object Parsing extends Serializable {

  val dPath = "/Users/burak.cakan/IdeaProjects/spark-training/environment/data/small"

    def parseRating(line: String): Rating = line.split(',').toList match {
      case userId :: movieId :: rating :: ts :: Nil =>
        Rating(
          userId.toInt,
          movieId.toInt,
          rating.toDouble,
          new Timestamp(ts.toLong * 1000)
        )
    }

    def parseMovie(line: String): Movie = {
      val splitted = line.split(",", 2)

      val id = splitted(0).toInt
      val remaining = splitted(1)
      val sp = remaining.lastIndexOf(",")
      val titleDirty = remaining.substring(0, sp)
      val title =
        if (titleDirty.startsWith("\"")) titleDirty.drop(1).init else titleDirty   // Filmin Adi

      val year = Try(
        title
          .substring(title.lastIndexOf("("), title.lastIndexOf(")"))
          .drop(1)
          .toInt
      ).toOption
      val genres = remaining.substring(sp + 1).split('|').toList
      Movie(id, title, year, genres)
    }

    def parseLink(line:String):Link = line.split(',').toList match {
      case movieId::imdbId::tmdbId::Nil => Link(movieId.toInt, imdbId.toInt, Some(tmdbId.toInt))
      case movieId::imdbId::Nil => Link(movieId.toInt, imdbId.toInt, None)
    }

    def parseTag(line:String):Tag = line.split(',').toList match {
      case userId::movieId::tag::ts::Nil => Tag(userId.toInt,movieId.toInt,tag,new Timestamp(ts.toLong * 1000))
    }


}
