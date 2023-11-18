package scraping

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken, RawHeader}
import akka.http.scaladsl.model.{HttpHeader, HttpMethods, HttpProtocols, HttpRequest, Uri}
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.javadsl.SourceQueueWithComplete
import akka.stream.scaladsl.{Keep, Sink, Source}
import cats.{FlatMap, Monad}
import com.typesafe.config.ConfigFactory
import database.Mediator

import java.time
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.{DurationInt, MILLISECONDS}
import io.circe._
import io.circe.parser._
import model.rtl.{Episode, EpisodeV2}
import org.slf4j.LoggerFactory
import reactor.core.publisher.FluxSink.OverflowStrategy
import scraping.ScrapeJob.ScrapeJob
import service.RtlClient

import java.time.temporal.{Temporal, TemporalAmount}
import java.time.{LocalDateTime, Month}
import java.util.UUID


/**
 * Scraper for a specific Series
 */
class SeriesScraper(seriesId: Int, name: String, rtlClient: RtlClient, mediator: Mediator) {
  private val config = ConfigFactory.load("reference.conf").getConfig("scraping").getConfig("SeriesScraper")
  private val SCRAPE_GROUP_SIZE = config.getInt("scrape_group_size")
  private val SCRAPE_GROUP_SECONDS = config.getInt("scrape_group_time").seconds
  private val SCRAPE_PER_SECOND = config.getInt("scrape_per_second")

  private val PARALLELISM = config.getInt("parallelism")
  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private val episodeBaseUrl = config.getString("episode_base_url")


  private lazy val rtlPlusToken = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ4N1RJT2o1bXd3T0daLS1fOVdjcmhDbzdHemVCTDgwOWQxZlByN29wUThBIn0.eyJleHAiOjE2OTgwOTE2NjUsImlhdCI6MTY5ODA3NzI2NSwianRpIjoiYzlkZDYyODAtOTJjNy00ZWNkLTk0ZjQtNWE4YzA2MWVmMTdlIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLnJ0bC5kZS9hdXRoL3JlYWxtcy9ydGxwbHVzIiwic3ViIjoiNWYyODFmOTAtOWM5OS00MzcwLWFmZDYtMTM1N2ZlMDc2N2YxIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYW5vbnltb3VzLXVzZXIiLCJhbGxvd2VkLW9yaWdpbnMiOlsiKiJdLCJzY29wZSI6IiIsImNsaWVudEhvc3QiOiI5NS4yMjMuMTA3LjE2MyIsImNsaWVudElkIjoiYW5vbnltb3VzLXVzZXIiLCJpc0d1ZXN0Ijp0cnVlLCJwZXJtaXNzaW9ucyI6eyJnZW5lcmFsIjp7InBvcnRhYmlsaXR5IjpmYWxzZSwiYWxwaGFWIjp0cnVlLCJtYXhBbW91bnRPZlByb2ZpbGVzIjo0LCJtYXhNcGFQcm9maWxlcyI6NCwic2V0UGluIjpmYWxzZSwibWF4RG93bmxvYWREZXZpY2VzIjowLCJhY2Nlc3NQcmVTYWxlIjpmYWxzZX0sInN0cmVhbWluZyI6eyJ2b2RBY2Nlc3NUb0ZyZWVDb250ZW50Ijp0cnVlLCJ2b2RBY2Nlc3NUb1BheUNvbnRlbnQiOmZhbHNlLCJsaXZlc3RyZWFtQWNjZXNzVG9GcmVlVHYiOmZhbHNlLCJsaXZlc3RyZWFtQWNjZXNzVG9QYXlUdiI6ZmFsc2UsImxpdmVzdHJlYW1BY2Nlc3NUb0Zhc3QiOnRydWUsInZvZFF1YWxpdHkiOiJMT1ciLCJsaXZlUXVhbGl0eSI6IkxPVyIsImZhc3RRdWFsaXR5IjoiTE9XIiwibWF4UGFyYWxsZWxTdHJlYW1zIjoxLCJsaXZlZXZlbnRBY2Nlc3NUb0ZyZWVUdiI6dHJ1ZSwibGl2ZWV2ZW50QWNjZXNzVG9QYXlUdiI6ZmFsc2V9LCJ3YXRjaEZlYXR1cmVzIjp7ImNvbnRlbnREb3dubG9hZCI6ZmFsc2UsIm9yaWdpbmFsVmVyc2lvbiI6ZmFsc2UsImNvbnRpbnVlV2F0Y2hpbmciOmZhbHNlLCJza2lwQWQiOmZhbHNlLCJkb2xieSI6ZmFsc2UsImJvb2ttYXJrV2F0Y2giOmZhbHNlfSwiYWR2ZXJ0aXNpbmciOnsibWF4UHJlUm9sbHMiOjMsIm1pZFJvbGxzIjp0cnVlLCJwb3N0Um9sbHMiOnRydWUsImNoYXB0ZXJzIjp0cnVlLCJzcGVjaWFsQWRzIjpmYWxzZSwiYnJlYWtBZHMiOmZhbHNlLCJhZFNjaGVtZSI6ImFkYV9mcmVlIiwidGVkUGF5QWR2ZXJ0aXNlbWVudCI6ZmFsc2V9LCJtdXNpYyI6eyJhY2Nlc3NNdXNpY0NvbnRlbnQiOmZhbHNlLCJhY2Nlc3NNdXNpY0NvbnRlbnRPdGhlclByb2ZpbGVzIjpmYWxzZSwiZGVlemVyT2ZmZXJDb2RlIjotMSwiZGVlemVyVHJpYWxPZmZlckNvZGUiOi0xLCJkZWV6ZXJNYXhQYXJhbGxlbFN0cmVhbXMiOjAsInZpZXdNdXNpY0NvbnRlbnQiOnRydWV9LCJwb2RjYXN0cyI6eyJib29rbWFya1BvZGNhc3RzIjpmYWxzZSwiYWNjZXNzRnJlZVBvZGNhc3RzIjp0cnVlLCJhY2Nlc3NQcmVtaXVtUG9kY2FzdHMiOmZhbHNlLCJmb2xsb3dQb2RjYXN0cyI6ZmFsc2UsImRvd25sb2FkUG9kY2FzdHMiOmZhbHNlLCJjb250aW51ZUxpc3RlbmluZ1BvZGNhc3RzIjpmYWxzZX0sInJhZGlvIjp7ImFjY2Vzc1JhZGlvQ29udGVudCI6dHJ1ZX0sIm1hZ2F6aW5lIjp7ImFydGljbGVDcmVkaXRzIjowLCJhY2Nlc3NNYWdhemluZUFydGljbGVzIjpmYWxzZSwiYnJhbmRTdWJzY3JpcHRpb25TbG90cyI6MCwiYm9va21hcmtNYWdhemluZSI6ZmFsc2V9LCJhdWRpb2Jvb2tzIjp7ImNhblJlZGVlbUNyZWRpdCI6ZmFsc2UsImNhblJlZGVlbUNyZWRpdE90aGVyUHJvZmlsZXMiOmZhbHNlLCJhY2Nlc3NEZWV6ZXJBdWRpb2Jvb2tzIjpmYWxzZSwiYWNjZXNzRGVlemVyQXVkaW9ib29rc090aGVyUHJvZmlsZXMiOmZhbHNlLCJhY2Nlc3NQcmhBdWRpb2Jvb2tzIjpmYWxzZSwiYWNjZXNzUHJoQXVkaW9ib29rc090aGVyUHJvZmlsZXMiOmZhbHNlLCJhY2Nlc3NCb3VnaHRQcmhBdWRpb2Jvb2tzIjpmYWxzZSwiYWNjZXNzQm91Z2h0UHJoQXVkaW9ib29rc090aGVyUHJvZmlsZXMiOmZhbHNlLCJwcmhDcmVkaXRzIjowLCJwcmhNYXhQYXJhbGxlbFN0cmVhbXMiOjB9LCJ0b2dnbyI6eyJza2lwQWR2ZXJ0aXNpbmciOmZhbHNlfX0sImNsaWVudEFkZHJlc3MiOiI5NS4yMjMuMTA3LjE2MyJ9.mR_BmbGZctud189vGyCgD4HCEyPOP9WsiefWAMECQTVjlq59sm2aH9YVPvfmnMmt_l-rNI9XoaxHzvBcSVdYdxtShcVoI894uU6RLWiIrYttZ022L99ut9j_Vs0Rg6hX_eueq-vJcemNHbwnSsRgH2f36BQ_vlLaN6z28pHaLPtPWs-q1jJ-tBC_MfVbSNb6rAJxVLUoda6l6B1YkpPmQrGxU2DJ2gmec4ssKrJCMghcg8NLfMvbjHq-v7aLt2OcbaNZqWpOqzEcvqEdd74Q5jKJCMnWjcK-OhpF1nmj-Ff52y6lIbG9JStWPMFu4pvP4Yd_IjCg11QLyos7XjkaZA"

  @Deprecated
  private val baseUrl = config.getString("base_url")
  @Deprecated
  private val scrapePath = baseUrl + seriesId.toString


  //new graphql api
  private val newBaseUrl = config.getString("new_base_url")
  private val persistedQueryHash = config.getString("persisted_query_hash")
  private val persistedQueryVersion = config.getString("persisted_query_version")

  private val TO_STRICT_TIMEOUT = config.getInt("to_strict_timeout").millis
  private lazy val scrapeHeaders = Seq(
    RawHeader("rtlplus-client-id", "rci:rtlplus:web"),
    RawHeader("rtlplus-client-version", "2023.11.16.3"),
    Authorization(OAuth2BearerToken(rtlPlusToken))
  )

  private val dateTimePattern = "\\d{2}.\\d{2}.\\d{4}, \\d{2}:\\d{2}".r
  private implicit val system: ActorSystem = ActorSystem("system-scraper-" + seriesId.toString)
  private implicit val executionContext: ExecutionContextExecutor = system.dispatcher


  private val scrapeSource = Source
    .queue[List[ScrapeJob]](100, akka.stream.OverflowStrategy.fail)
    .flatMapConcat(scrapeWorker)
    .groupedWithin(SCRAPE_GROUP_SIZE, SCRAPE_GROUP_SECONDS)



  //TODO: use queue
  //TODO: queue for scraping AND db upload should not be part of Scraper
  //      queue should be created with instance of SeriesScraper (and possibly Mediator) as argument
  private val queue: akka.stream.scaladsl.SourceQueueWithComplete[List[ScrapeJob]] = Source
    .queue[List[ScrapeJob]](1000, akka.stream.OverflowStrategy.fail)
    .flatMapConcat(scrapeWorker)
    .groupedWithin(SCRAPE_GROUP_SIZE, SCRAPE_GROUP_SECONDS)
    .map(group => databaseWorker(group.toList.flatten)).map(_.map(_.map(dbRes => {
      val msg = if (dbRes._2) "Database upload failed." else "Database upload successful."
      LOGGER.debug(s"[Scraper-$seriesId] $msg {Episode ${dbRes._1.episode}, Season ${dbRes._1.season}, Series ${dbRes._1.seriesId}}")
    })))//.mapAsync(1)(group => databaseWorker(group.flatten.toList))
    .toMat(Sink.ignore)(Keep.left)
    .run()
  //val a = queue .offer(ScrapeJob(1,1)).

  /**
   * writes query parameters for graphql query
   * @param season
   * @param maxEpisodes
   * @param offset
   * @return
   */
  private def scrapeQuery(season: Int, maxEpisodes: Option[Int], offset: Option[Int]): String = {
    val operationName = "SeasonWithFormatAndEpisodes"
    val seasonId = "172157"
    val maxCount = maxEpisodes match {
      case None => 500
      case Some(cnt) => if(cnt > 500) 500 else cnt
    }
    val variables = s"%7B%22seasonId%22:%22rrn:watch:videohub:season:$seasonId%22,%22offset%22:$offset,%22limit%22:$maxCount%7D"
    val extensions = s"%7B%22persistedQuery%22:%7B%22version%22:$persistedQueryVersion,%22sha256Hash%22:%22$persistedQueryHash%22%7D%7D"

    val query = s"?operationName=$operationName&variables=$variables&extensions=$extensions"
    query
  }

  /**
   * Starts scraping given a season and optional list of episodeNrs
   * @param season
   * @param maxEpisodes if none, scrapes all episodes (<= 500
   * @return
   * //TODO implement
   */
  private def scrapeSeason(season: Int, maxEpisodes: Option[Int], offset: Option[Int]): Future[List[EpisodeV2]] = {
    LOGGER.info(s"[Scraper-$seriesId] Scraping season $season, maxEpisodes $maxEpisodes")
    val url = newBaseUrl + scrapeQuery(season, maxEpisodes, offset)
    val req = HttpRequest(HttpMethods.GET, uri = url, headers = scrapeHeaders, protocol = HttpProtocols.`HTTP/2.0`)
    val response = rtlClient.send(req)
    val episodes = response.flatMap(resp => {
      resp.entity.toStrict(TO_STRICT_TIMEOUT)
        .map(r => {
          parse(r.data.utf8String) match {
            case Right(json) => {
              val eps = json \\ "episodes"
              eps.map(ep => {
                val id = (ep \\ "id").head.asString.getOrElse("no id available")
                val title = (ep \\ "title").head.asString.getOrElse("no title available")
                val isPremium = (ep \\ "tier").head.asBoolean
                val number = (ep \\ "number").head.asNumber.flatMap(_.toInt).getOrElse(-1)
                val ageRating = (ep \\ "ageRating").head.asNumber.flatMap(_.toInt).getOrElse(-1)
                val description = (ep \\ "description").head.asString.getOrElse("no description available")
                val imageUrl = ((((ep \\ "watchImages").head \\ "default").head) \\ "absoluteUri").head.asString.getOrElse("no image url available")
                val epUrl = ((ep \\ "urlData").head \\ "watchPath").head.asString.getOrElse("no description available")
                val durationSeconds = (ep \\ "durationInSecondsV2").head.asNumber.flatMap(_.toInt).getOrElse(-1)
                EpisodeV2(id, title, isPremium, number, season, ageRating, description, imageUrl, epUrl, durationSeconds)
              })
            }
            case Left(failure) => {
              LOGGER.error(s"Parsing graphql response failed. Failure: $failure")
              Nil
            }
          }
        })
    })
  }


  @Deprecated
  /**
   * Starts scraping given month and year
   *  @deprecated Function is replaced by scrapeSeason(season: Int, episodes: Option[Range])
   */
  private def scrapeMonth(month: Int, year: Int): Future[List[Episode]] = {
    LOGGER.info(s"[Scraper-$seriesId] Scraping month $month, year $year")
    //val uri = Uri.from(scrapePath).withQuery(Query("year" -> year.toString, "month" -> month.toString))
    val url = scrapePath + s"?year=$year&month=$month"
    val req = HttpRequest(HttpMethods.GET, uri = url )
    //val req = HttpRequest(uri = uri)
    val response = rtlClient.send(req)
    val episodes = response.flatMap(resp =>{
      resp.entity.toStrict(500.millis)
        .map(r => {
          parse(r.data.utf8String) match{
            case Right(json) => {
              //.map(json => {
              val items = json \\ "items"
              items.flatMap(itemList => {
                itemList.asArray.getOrElse(Nil).toList.map(item => {
                  val name = (item \\ "subheadline").head.toString().dropRight(1).drop(1)
                  val url = episodeBaseUrl + (item \\ "url").head.toString().dropRight(1).drop(1)
                  val txt = (item \\ "text").last

                  //TODO: test images, add description...
                  val description = (item \\ "")
                  val images = (item \\ "images").head.asArray.getOrElse(Vector.empty[Json]).map(image => image.asObject.map(imgObj => imgObj.toMap.map(kv => (kv._1, kv._2.toString))).getOrElse(Map.empty[String, String])).toList
                  val dateTime = dateTimePattern.findFirstMatchIn(txt.toString()).map(mtch => {
                    val matchStr = mtch.toString()
                    val y = matchStr.slice(6, 10).toIntOption.getOrElse(0)
                    val m = matchStr.slice(3, 5).toIntOption.getOrElse(0)
                    val d = matchStr.slice(0, 2).toIntOption.getOrElse(0)
                    val h = matchStr.slice(12, 14).toIntOption.getOrElse(0)
                    val min = matchStr.slice(15, 17).toIntOption.getOrElse(0)
                    LocalDateTime.of(y, m, d, h, min)
                  }) getOrElse (LocalDateTime.of(0, 1, 1, 0, 0))
                  val (episode, season) = parseEpisodeSeason(txt.toString())
                  Episode(season, episode, name, seriesId, dateTime, url, images) //make map from list, with dimension as key
                })
              })
            }
            case Left(failure) => Nil
          }

        })

    })
    episodes
  }
  
  private def parseEpisodeSeason(txt: String): (Int,Option[Int]) = {
    val str = txt.split("\\|").head.replace(",", "")
    val parts = str.split(" ").filter(part => part.forall(Character.isDigit))
    return (parts.last.toInt, parts.head.toIntOption)
  }
  private def scrapeWorker(jobs: List[ScrapeJob]): Source[List[Episode], NotUsed] = {
    Source(jobs)
      .throttle(SCRAPE_PER_SECOND, 1.second)
      .mapAsync(PARALLELISM)(job => scrape(job))
  }
  def scrape(job: ScrapeJob): Future[List[Episode]] = {
    LOGGER.info(s"[ScrapeWorker-$seriesId] Started job ${job.jobId}\n" ++
                s"\tjob: {months: ${job.months.toString}, year: ${job.years.toString}}")
    Future.sequence(job.years.map(year => {
      Future.sequence(job.months.map(month => {
        scrapeMonth(month, year)
      }).toList)
    }).toList).map(_.flatten.flatten)
  }

  private def databaseWorker(episodes: List[Episode]): Future[List[(Episode, Boolean)]] = {
    Future(mediator.addEpisodes(episodes))
  }
}

trait Job {
  val jobId: String
  val creationTimestamp: LocalDateTime
  def ageMillis: Long = creationTimestamp.until(LocalDateTime.now, MILLISECONDS.toChronoUnit)
}

abstract class ScrapeInstant(tuple: (Int, Int)) {

}
case class ScrapeDate(month: Int, year: Int) extends ScrapeInstant((month, year))
case class ScrapeSeason()
abstract class AbstractRange(range: Range){
  def flatMap[B](f: Int => IterableOnce[B]): List[B] = {
    range.flatMap(f).toList
  }
  def map[B](f: Int => B): List[B] = {
    range.map(f).toList
  }

  def x (otherRange: AbstractRange): ScrapeInstant = {

  }
}
case class MonthRange(months: Range) extends AbstractRange(months)
case class YearRange(years: Range) extends AbstractRange(years)
case class EpisodeRange(episodes: Range) extends AbstractRange(episodes)
case class SeasonRange(seasons: Range) extends AbstractRange(seasons)

abstract class ScrapeRange(r1: AbstractRange, r2: AbstractRange) {
  def tuples: List[(Int, Int)] = {
    Range.inclusive(0,3).flatMap(x => List(x))
    r1.flatMap
  }
}
case class ScrapeDates(months: MonthRange, years: YearRange) extends ScrapeRange{

}
object ScrapeJob{
  case class ScrapeJob(jobId: String, months: Range, years: Range, creationTimestamp: LocalDateTime) extends Job
  def apply(
           jobId: String,
           month: Int,
           year: Int
           ): ScrapeJob = {
    ScrapeJob(jobId, Range.inclusive(month, month), Range.inclusive(year, year), LocalDateTime.now)
  }
  def apply(
           jobId: String,
           months: Range,
           years: Range
           ): ScrapeJob = {
    ScrapeJob(jobId, months, years, LocalDateTime.now)
  }

  def apply(
           jobId: String,
           years: Range
           ): ScrapeJob = {
    ScrapeJob(jobId, Range.inclusive(1, 12), years, LocalDateTime.now)
  }
  def apply(
           month: Int,
           year: Int
           ): ScrapeJob = {
    ScrapeJob(java.util.UUID.randomUUID.toString, Range.inclusive(month, month), Range.inclusive(year, year), LocalDateTime.now)
  }
  def apply(
           months: Range,
           years: Range
           ): ScrapeJob = {
    ScrapeJob(java.util.UUID.randomUUID.toString, months, years, LocalDateTime.now)
  }
  def apply(
           years: Range
           ): ScrapeJob = {
    ScrapeJob(java.util.UUID.randomUUID.toString, Range.inclusive(1,12), years, LocalDateTime.now)
  }
}
