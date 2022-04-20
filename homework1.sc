import java.util.Date
import scala.io.Source
import scala.util.Try

object dataParseAndExtract{
  case class Movie(
                    id: Int,
                    title: String,
                    year: Option[Int],
                    genres: List[String]
                  )

  case class Rating(userId: Int, movieId: Int, rating: Double, ts: Date)

  case class Tags(userId: Int, movieId: Int, tag: String, ts:Date)

  def parseTags(line: String): Tags = line.split(',').toList match {
    case userId :: movieId :: tag :: ts :: Nil =>
      Tags(
        userId.toInt,
        movieId.toInt,
        tag.toString,
        new Date(ts.toLong * 1000),
      )
  }

  def parseRating(line: String): Rating = line.split(',').toList match {
    case userId :: movieId :: rating :: ts :: Nil =>
      Rating(
        userId.toInt,
        movieId.toInt,
        rating.toDouble,
        new Date(ts.toLong * 1000),
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


  val ratingLines =
    Source.fromFile("/Users/burak.cakan/IdeaProjects/scalatraining/src/main/scala/hw/ratings.csv").getLines().toList.drop(1)

  val movieLines =
    Source.fromFile("/Users/burak.cakan/IdeaProjects/scalatraining/src/main/scala/hw/movies.csv").getLines().toList.drop(1)

  val tagLines =
    Source.fromFile("/Users/burak.cakan/IdeaProjects/scalatraining/src/main/scala/hw/tags.csv").getLines().toList.drop(1)

  val movies = movieLines.map(parseMovie)
  val movieMap = movies.map(m=> (m.id,m)).toMap
  val ratings = ratingLines.map(parseRating)


  val tags = tagLines.map(parseTags)
  val tagMap = tags.map(i => (i.movieId,i)).toMap

  val groupedTags = tags.groupBy(r => r.movieId).toList

  val groupedMovies =  movies.groupBy(r => r.id)

  val groupedRatings = ratings.groupBy(r => r.movieId)

  val averageRatings = groupedRatings.map { case (k,v) =>
    (k,v.map(_.rating).sum,v.length)
  }.toList.sortBy(_._2).reverse.take(10)


  val result4 = groupedRatings.map( gr => (gr._2,movieMap(gr._1).year))
  val ratingswGenres = groupedRatings.map( gr => (gr._2,movieMap(gr._1).genres,movieMap(gr._1).year))


  //user id ve movie id ile joinleme:
  val tagMapped = tags.map(i => ((Option(i.movieId), Option(i.userId)),i)).toMap
  val rat = ratings.map(i => ((Option(i.movieId), Option(i.userId)),i)).toMap

  val yearRatings2 = result4.map {
    case (k,v) => (k.map(_.rating), v)
  }.toList

   def bestYears(movies: List[Movie]) = {
    val years = movies.flatMap(_.year)
    val groupedYears =
      years.groupBy(i => i).map { case (k, v) => (k, v.length) }.toList
    groupedYears
      .sortBy(_._2)
      .reverse
      .foreach(println)
  }

   def distinctGenres(movies: List[Movie]) = {
    val genres = movies.flatMap(_.genres)
    val countGenres = genres.groupBy(i => i)
    val grouped = countGenres.map { case (k, v) => (k, v.length) }.toList
    grouped.sortBy(_._2).reverse.foreach(println)
  }

  def minGenres(movies: List[Movie]) = {
    val genres = movies.flatMap(_.genres)
    val countGenres = genres.groupBy(i => i)
    val countFilms = movies.groupBy(i => (i.id)).toList.length
    val grouped = countGenres.map { case (k, v) => (k, v.length) }.toList
    val filtered = grouped.filter(_._1 != "(no genres listed)" )
    val minGenre = filtered.sortBy(_._2).head //foreach(println)
    Array(minGenre,countFilms).foreach(println)
  }

  def goldenYear(rates:  List[(List[Double], Option[Int])]
                ): Unit = {
    val groupedGY = rates.  //dataParseAndExtract.yearRatings2.
      flatMap(row => row._1.map(number => (number, row._2))).
      groupBy( col => (col._1,col._2)).
      map{ case (k,v) => (k._1,k._2,v.length)}.toList

    val maxRate= groupedGY.filter(_._1==5.0)

    maxRate.sortBy(_._3).reverse.foreach(println)
  }

}

//Question 1:
println("The number of films according to the years:")
println(dataParseAndExtract.bestYears(dataParseAndExtract.movies))
println("The number of films according to the genres:")
println(dataParseAndExtract.distinctGenres(dataParseAndExtract.movies))

//Question 2:

println("The Golden Year (the year of most max-rate) is:")
println(dataParseAndExtract.goldenYear(dataParseAndExtract.yearRatings2))

// Quesiton 3:

println("The genre having minimum film number, the number of films in that genre and total number of films in overall")
dataParseAndExtract.minGenres(dataParseAndExtract.movies)

// Question 4:

dataParseAndExtract.ratingswGenres

val a1 = dataParseAndExtract.ratingswGenres.toList.flatMap(row => row._2.map( nm => (nm, row._1,row._3)))

val a2 = a1.flatMap(row => row._2.map( nm => (row._1,nm.rating,nm.userId,row._3)))

println("(Genre|Year) --> (# of rating|# of person tagged|the rate) ") //this rate is always 1
a2.groupBy(t => (t._1, t._4))
  .view
  .mapValues(lst => (lst.filter(!_._2.isNaN).length, lst.filter(!_._3.isNaN).length)) // transform grouping into tuple with needed counts
  .mapValues(t => (t._1, t._2, t._1.toFloat / t._2)) // "add" 3rd column
  .toMap.foreach(println)

// Question 5:

val mostTagged = dataParseAndExtract.tags.groupBy(r => r.userId).map { case(k,v) => (k,v.length)}.toList.sortBy(i => i._2).reverse.head
val mostTaggedPerson = mostTagged._1.toString
val mostTagTimes = mostTagged._2.toString

println(s"The most tagged user id is ${mostTaggedPerson} with ${mostTagTimes} times of tagging")

val grouped = a2.groupBy(t => (t._1, t._4)).flatMap( row => row._2.map( r => (row._1,r._2,r._3) ))
val filtered = grouped.filter(_._3==mostTagged._1).toList

println("Drama in 2002 is the best for user id=474:")
filtered.groupBy(_._1).map(group => (group._1, group._2.map(_._2).sum)).toList.sortBy(i => i._2).reverse.head

println("sci-fi in 1975 is the worst for this user")
filtered.groupBy(_._1).map(group => (group._1, group._2.map(_._2).sum)).toList.sortBy(i => i._2).head

//Question 6:

val result  = Try(dataParseAndExtract.rat.map(ar => (dataParseAndExtract.tagMapped(ar._1).userId,dataParseAndExtract.tagMapped(ar._1).movieId,dataParseAndExtract.tagMapped(ar._1).ts,ar._2))
).toOption
//burada user id ve movie id ile eşleyemediği için none geldi. O nedenle 6. soruya devam edemedim