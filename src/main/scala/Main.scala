import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import database.Mediator
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import org.slf4j.LoggerFactory
import scraping.{ScrapeJob, SeriesScraper}
import search.Searchinator
import service.RtlClient

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object Main {

  /**
   * Yes. This is the interface for now.
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load("reference.conf").getConfig("application")
    val LOGGER = LoggerFactory.getLogger(this.getClass)
    val conf = config.getConfig("main")
    val appName = conf.getString("app-name")
    println(appName)

    implicit val actorSystem: ActorSystem = ActorSystem("system-main")
    implicit val executionContext: ExecutionContext = actorSystem.dispatcher

    val search = new Searchinator()

    val token = AuthTokens.basic("rtl-mediator", "mediator")

    val driver = GraphDatabase.driver("bolt://localhost/7687/episodes", AuthTokens.basic("mediator", "mediator"))
    val mediator = new Mediator(driver)

    val rtl = new RtlClient()
    val scraper = new SeriesScraper(247, "test", rtl, mediator)
    val job1 = ScrapeJob(years = Range.inclusive(2016, 2023))
    val res = scraper.scrape(job1)
    res.onComplete {
      case Success(eps) => mediator.addEpisodes(eps, update = false)
      case Failure(e) => LOGGER.error(s"[main] ScrapeJob ${job1.jobId} failed.")
    }
  }
}