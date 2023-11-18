package service

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import org.slf4j.LoggerFactory
import scraping.SeriesScraper

import scala.concurrent.Future


class RtlClient {
  private implicit val system: ActorSystem = ActorSystem("system-rtlClient")
  private val httpClient = Http()
  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private val scrapers: List[SeriesScraper] = List()

  def send(req: HttpRequest): Future[HttpResponse] = {
    LOGGER.info(s"Sending Request: ${req.toString}")
    httpClient.singleRequest(req)
  }

  def enqueue(job: Job)


}
