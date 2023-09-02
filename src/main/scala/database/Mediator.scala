package database

import akka.actor.ActorRef
import akka.stream.scaladsl.Source
import model.rtl.Episode
import org.neo4j.driver.{Driver, GraphDatabase, Result}

import java.util.stream.Collectors
import scala.concurrent.Future
import scala.util.control.Exception
import scala.jdk.CollectionConverters._
import model.search._
import org.slf4j.LoggerFactory

import java.time.{LocalDateTime, ZoneId}

/**
 * Interacts with neo4j database
 */
class Mediator(driver: Driver) {
  private val session = driver.session
  private val LOGGER = LoggerFactory.getLogger(this.getClass)


  private val queue = Source.queue[DbJob](10)

  def testAdd(): Unit = {
    val ep = Episode(Some(1), 2, "test2", 0, LocalDateTime.MIN)
    val script =
      s"MERGE (ep: Episode {name: '${ep.name}', dateTime: '${ep.datetime.toString}', nr: ${ep.episode}})" ++
        s" ON MATCH SET ep.log = 'match'" ++
        s" ON CREATE SET ep.log = 'create'" ++
        s" ON CREATE SET ep.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
        s" MERGE (series: Series {seriesId: ${ep.seriesId}})" ++
        s" ON MATCH SET series.log = 'match'" ++
        s" ON CREATE SET series.log = 'create'" ++
        s" ON CREATE SET series.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}"

    val fullScript = ep.season match {
      case Some(s) => script ++ s" MERGE (season: Season {nr: $s, seriesId: ${ep.seriesId}})" ++
        s" ON MATCH SET season.log = 'match'" ++
        s" ON CREATE SET season.log = 'create'" ++
        s" ON CREATE SET season.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
        s" MERGE (ep)-[seasonRel:BELONGS_TO_SEASON]->(season)" ++
        s" ON MATCH SET seasonRel.log = 'match'" ++
        s" ON CREATE SET seasonRel.log = 'create'" ++
        s" ON CREATE SET seasonRel.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
        s" MERGE (season)-[seriesRel:BELONGS_TO_SERIES]->(series)" ++
        s" ON MATCH SET seriesRel.log = 'match'" ++
        s" ON CREATE SET seriesRel.log = 'create'" ++
        s" ON CREATE SET seriesRel.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
        s" RETURN ep.log, season.log, series.log, seasonRel.log, seriesRel.log"
      case None => script ++ s"MERGE (season: Season {nr: 0, seriesId: ${ep.seriesId}})" ++
        s" ON MATCH SET season.log = 'match" ++
        s" ON CREATE SET season.log = 'create'" ++
        s" ON CREATE SET season.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
        s" MERGE (ep)-[seasonRel:BELONGS_TO_SEASON]->(season)" ++
        s" ON MATCH SET seasonRel.log = 'match'" ++
        s" ON CREATE SET seasonRel.log = 'create'" ++
        s" ON CREATE SET seasonRel.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
        s" MERGE (season)-[seriesRel:BELONGS_TO_SERIES]->(series)" ++
        s" ON MATCH SET seriesRel.log = 'match'" ++
        s" ON CREATE SET seriesRel.log = 'create'" ++
        s" ON CREATE SET seriesRel.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
        s" RETURN ep.log, season.log, series.log, seasonRel.log, seriesRel.log"
    }

    val transaction = session.beginTransaction()
    val result = transaction.run(fullScript)
    val res = result.list(rec => rec.fields().asScala.toList.map(pair => (pair.key(), pair.value().asString()))).asScala.toList.flatten
    val success = res.exists(kv => kv._2 == "fail")
    if(success) {
      transaction.commit()
    }
    transaction.close()
  }

  def addEpisodes(episodes: List[Episode], update: Boolean = true): List[(Episode,Boolean)] = {
    var results = List.empty[(Episode, Boolean)]

    val epCount = episodes.length
    val transaction = session.beginTransaction()
    if(update){
      //TODO: update
      episodes.map((_, false))
    }else{
      try {
        results = episodes.map(ep => {
          val script =
            s"MERGE (ep: Episode {name: '${ep.name}', dateTime: '${ep.datetime.toString}', nr: ${ep.episode}})" ++
              s" ON MATCH SET ep.log = 'match'" ++
              s" ON CREATE SET ep.log = 'create'" ++
              s" ON CREATE SET ep.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
            s" MERGE (series: Series {seriesId: ${ep.seriesId}})" ++
              s" ON MATCH SET series.log = 'match'" ++
              s" ON CREATE SET series.log = 'create'" ++
              s" ON CREATE SET series.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}"

          val fullScript = ep.season match {
            case Some(s) => script ++ s" MERGE (season: Season {nr: $s, seriesId: ${ep.seriesId}})" ++
              s" ON MATCH SET season.log = 'match'" ++
              s" ON CREATE SET season.log = 'create'" ++
              s" ON CREATE SET season.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
              s" MERGE (ep)-[seasonRel:BELONGS_TO_SEASON]->(season)" ++
              s" ON MATCH SET seasonRel.log = 'match'" ++
              s" ON CREATE SET seasonRel.log = 'create'" ++
              s" ON CREATE SET seasonRel.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
              s" MERGE (season)-[seriesRel:BELONGS_TO_SERIES]->(series)" ++
              s" ON MATCH SET seriesRel.log = 'match'" ++
              s" ON CREATE SET seriesRel.log = 'create'" ++
              s" ON CREATE SET seriesRel.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
              s" RETURN ep.log, season.log, series.log, seasonRel.log, seriesRel.log"
            case None => script ++ s"MERGE (season: Season {nr: 0, seriesId: ${ep.seriesId}})" ++
            s" ON MATCH SET season.log = 'match" ++
            s" ON CREATE SET season.log = 'create'" ++
            s" ON CREATE SET season.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
            s" MERGE (ep)-[seasonRel:BELONGS_TO_SEASON]->(season)" ++
            s" ON MATCH SET seasonRel.log = 'match'" ++
            s" ON CREATE SET seasonRel.log = 'create'" ++
            s" ON CREATE SET seasonRel.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
            s" MERGE (season)-[seriesRel:BELONGS_TO_SERIES]->(series)" ++
            s" ON MATCH SET seriesRel.log = 'match'" ++
            s" ON CREATE SET seriesRel.log = 'create'" ++
            s" ON CREATE SET seriesRel.creationTimestamp = ${LocalDateTime.now(ZoneId.of("UTC+2")).toString}" ++
            s" RETURN ep.log, season.log, series.log, seasonRel.log, seriesRel.log"
          }

          val result = transaction.run(fullScript)
          val res = result.list(rec => rec.fields().asScala.toList.map(pair => (pair.key(), pair.value().asString()))).asScala.toList.flatten
          val success = !res.exists(kv => kv._2 == "fail")

          (ep,success)

        })
        if(results.contains(false)) {
          transaction.commit()
          LOGGER.info(s"[Mediator] Transaction committed for $epCount episodes")
        }else{
          LOGGER.error(s"[Mediator] Encountered errors. Changes will be rolled back.")
        }

        results
      }
      catch {
        case exception: Exception => {
          transaction.rollback()
          val results = episodes.map(ep => (ep, false))
          results
        }
      }finally{
        transaction.close()
      }






    }
  }
  private def checkResult(result: DbResult):Boolean = {
    result match{
      case AddResult(epLog, seasonLog, seriesLog, seasonRel, seriesRel) => !(epLog.equals("fail") || seasonLog.equals("fail") || seriesLog.equals("fail") || seasonRel.equals("fail") || seriesRel.equals("fail"))
    }
  }

}

abstract class DbResult
case class AddResult(epLog: String, seasonLog: String, seriesLog: String, seasonRel: String, seriesRel: String) extends DbResult

abstract class DbJob
case class AddJob(episodes: List[Episode]) extends DbJob
case class UpdateJob(episodes: List[Episode]) extends DbJob
case class DeleteJob(episodes: List[Episode]) extends DbJob
case class SearchJob(query: SearchQuery)