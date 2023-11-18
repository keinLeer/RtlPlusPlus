package model.search

trait SearchQuery {

}
trait TextQuery extends SearchQuery {
    val txt: Option[String]
}

trait FilterQuery extends SearchQuery {
    val tags: Map[String, List[String]]
}

trait RangeQuery extends SearchQuery {
    val seriesId: List[Int]
    val episode: List[(Int, Int)]
}
case class EpisodeQuery( txt: Option[String], tags: Map[String, List[String]], seriesId: List[Int] = Nil, episode: List[(Int,Int)] = Nil)
  extends FilterQuery with TextQuery with RangeQuery
case class SeriesQuery(txt: Option[String], tags: Map[String, List[String]])
  extends FilterQuery with TextQuery

private case class EpisodeNLQ(txt: Option[String])
  extends TextQuery
object EpisodeNLQ {
    def apply(str: String): EpisodeNLQ = {
        if(str == ""){
            new EpisodeNLQ(Option.empty[String])
        }else{
            new EpisodeNLQ(Some(str))
        }
    }
}

