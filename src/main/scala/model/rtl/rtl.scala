package model.rtl

import java.time.LocalDateTime


case class Episode(season: Option[Int], episode: Int, name: String, seriesId: Int, datetime: LocalDateTime, url: String, images: Map[String, String])

case class EpisodeV2(id: String, title: String, isPremium: Option[Boolean], episode: Int, season: Int, ageRating: Int, description: String, imageUrl: String, epUrl: String, durationSeconds: Int)

case class Series(name: String, seriesId: Int)

