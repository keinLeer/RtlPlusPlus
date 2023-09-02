package model.search

case class SearchQuery()
case class EpisodeQuery(seriesId: List[Int] = Nil, episode: List[(Int,Int)] = Nil, txt: Option[String], tags: Map[String, List[String]])
case class SeriesQuery(txt: Option[String], tags: Map[String, List[String]])
