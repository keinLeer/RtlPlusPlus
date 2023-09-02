package model.rtl

import java.time.LocalDateTime


case class Episode(season: Option[Int], episode: Int, name: String, seriesId: Int, datetime: LocalDateTime)
case class Series(name: String, seriesId: Int)