package api

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, ResponseEntity, StatusCode, StatusCodes}
import akka.http.scaladsl.model.ResponseEntity.fromJava
import akka.http.scaladsl.{Http, unmarshalling}
import akka.http.scaladsl.server.Directives.{as, complete, concat, decodeRequest, entity, get, path, pathSingleSlash, post}
import akka.http.scaladsl.unmarshalling.{FromRequestUnmarshaller, Unmarshaller}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.headers.`Access-Control-Allow-Origin`
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.util.FastFuture
import database.Mediator
import model.rtl.Episode
import model.search.{EpisodeNLQ, EpisodeQuery}
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.concurrent.duration.DurationInt

class WebAppAPI(mediator: Mediator) {
  implicit val system = ActorSystem("WebAppAPI")
  implicit val executionContext = system.dispatcher
  implicit val filterFormat = jsonFormat2(Filter.apply)
  implicit val searchRequestFormat = jsonFormat2(SearchRequest.apply)
  implicit val um: FromRequestUnmarshaller[SearchRequest] = Unmarshaller(_ => request => {
    val jsonBody = request.entity.toStrict(500.millis).map(strictEntity => strictEntity.data.utf8String.parseJson)
    jsonBody.map(jsB => jsB.convertTo[SearchRequest])
  })
  //implicit val um: FromRequestUnmarshaller[SearchRequest] = Unmarshaller(_ => request => {
  //  val jsonBody = request.entity.toStrict(500, system).toString.parseJson
  //  FastFuture.successful(jsonBody.convertTo[SearchRequest])
  //})
  implicit val episodeFormat = jsonFormat5(ApiEpisode)
  //implicit  val episodeWriter: JsonWriter[ApiEpisode] =

  private val searchRoute: Route =
    path("search") {
      post {
        entity(as[SearchRequest]) { searchRequest =>
          val response = process(searchRequest)
          complete(response)
        }
      }
    }


  //TODO done for now, back to frontend

  private val server = Http().newServerAt("localhost", 8080).bind(searchRoute)
  server.map { _ =>
    println("Sucessfully started on http://localhost:8080")
  }recover{
    case ex =>
      println("Failed to start the server due to: " + ex.getMessage)
  }

  private def process(request: SearchRequest): HttpResponse = {
    if(request.filter sameElements Array.empty[Filter]){
      val result = mediator.search(EpisodeNLQ(request.text))
      val jsonResult = Map("resList" -> result.map(ep => Constructors.apiEpisode(ep).toJson)).toJson
      val resp = HttpResponse(status = StatusCodes.OK, headers = Seq(`Access-Control-Allow-Origin`("http://localhost:3000")), entity = jsonResult.toString)
      resp
    }else{
      val tags: Map[String, List[String]] = request.filter.filter(f => f.attribute == "tag").map(f =>{(f.attribute,f.value)}).toMap
      val result = if(request.text == ""){
        mediator.search(EpisodeQuery(None, tags, Nil, Nil))
      }else{
        mediator.search(EpisodeQuery(Some(request.text), tags, Nil, Nil))
      }
      val jsonResult = Map("resList" -> result.map(ep => Constructors.apiEpisode(ep).toJson)).toJson
      val resp = HttpResponse(status = StatusCodes.OK, headers  = Seq(`Access-Control-Allow-Origin`("http://localhost:3000")), entity = jsonResult.toString)
      resp
    }
  }


}

final case class SearchRequest(text: String, filter: List[Filter])
final case class Filter(attribute: String, value: List[String])
final case class ApiEpisode(season: Option[Int], episode: Int, name: String, seriesId: Int, datetime: String)

object Constructors {
  def apiEpisode(episode: Episode): ApiEpisode = {
    val datetime = episode.datetime.format(DateTimeFormatter.ISO_LOCAL_TIME)
    return ApiEpisode(episode.season, episode.episode, episode.name, episode.seriesId, datetime)
  }
}
