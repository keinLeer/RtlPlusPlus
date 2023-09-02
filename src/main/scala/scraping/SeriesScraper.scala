package scraping

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.javadsl.SourceQueueWithComplete
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.ConfigFactory
import database.Mediator

import java.time
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import io.circe._
import io.circe.parser._
import model.rtl.Episode
import org.slf4j.LoggerFactory
import reactor.core.publisher.FluxSink.OverflowStrategy
import service.RtlClient

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
  private val scrapePath = config.getString("base_url") + seriesId.toString
  private val dateTimePattern = "\\d{2}.\\d{2}.\\d{4}, \\d{2}:\\d{2}".r
  private implicit val system: ActorSystem = ActorSystem("system-scraper-" + seriesId.toString)
  private implicit val executionContext: ExecutionContextExecutor = system.dispatcher



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
                  val url = (item \\ "url").head
                  val txt = (item \\ "text").last
                  val images = (item \\ "images").head
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
                  Episode(season, episode, name, seriesId, dateTime)
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
                "\tjob: {months: ${job.months.toString}, year: ${job.years.toString}")
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

case class ScrapeJob(jobId: String, months: Range, years: Range)
object ScrapeJob {
  def apply(
           jobId: String,
           month: Int,
           year: Int
           ): ScrapeJob = {
    ScrapeJob(jobId, Range.inclusive(month, month), Range.inclusive(year, year))
  }
  def apply(
           jobId: String,
           months: Range,
           years: Range
           ): ScrapeJob = {
    ScrapeJob(jobId, months, years)
  }

  def apply(
           jobId: String,
           years: Range
           ): ScrapeJob = {
    ScrapeJob(jobId, Range.inclusive(1, 12), years)
  }
  def apply(
           month: Int,
           year: Int
           ): ScrapeJob = {
    ScrapeJob(java.util.UUID.randomUUID.toString, Range.inclusive(month, month), Range.inclusive(year, year))
  }
  def apply(
           months: Range,
           years: Range
           ): ScrapeJob = {
    ScrapeJob(java.util.UUID.randomUUID.toString, months, years)
  }
  def apply(
           years: Range
           ): ScrapeJob = {
    ScrapeJob(java.util.UUID.randomUUID.toString, Range.inclusive(1,12), years)
  }
}
