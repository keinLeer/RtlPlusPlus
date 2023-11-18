package database

import akka.actor.ActorRef
import akka.stream.scaladsl.Source
import api.CypherReader.CypherResult
import api.CypherWriter.{CypherQuery, CypherQueryElement}
import model.rtl.Episode
import org.neo4j.driver.{Driver, GraphDatabase, Query, Result, TransactionConfig, Value, Values}

import java.util.stream.Collectors
import scala.concurrent.Future
import scala.util.control.Exception
import scala.jdk.CollectionConverters._
import model.search._
import org.neo4j.driver.internal.value.ObjectValueAdapter
import org.neo4j.driver.util.Pair
import org.slf4j.LoggerFactory

import java.time.{LocalDateTime, ZoneId}
import java.util.Optional

/**
 * Interacts with neo4j database
 */
class Mediator(driver: Driver) {
  //private val session = driver.session
  private val LOGGER = LoggerFactory.getLogger(this.getClass)


  private val queue = Source.queue[DbJob](10)

  def execute(cypherQuery: CypherQuery): CypherResult = {
    val query = new Query(cypherQuery.toString)//cypherQuery.asInstanceOf[AnyRef] //new java.lang.Object() //CypherQuery.from(List.empty[CypherQueryElement])
    val metadata = Map[String, AnyRef]("query" -> cypherQuery.toString).asJava

    val transactionConfig = TransactionConfig.builder().withMetadata(metadata).build
    cypherQuery
    //start session
    val session = driver.session

    val transaction = session.beginTransaction(transactionConfig)
    val result = try{
      transaction.run(query)
    }
    val res = CypherResult.parse(result)
    val records = result.list().asScala.toList
    transaction.rollback()
    res
  }

  def addEpisodes(episodes: List[Episode], update: Boolean = true): List[(Episode,Boolean)] = {
    var results = List.empty[(Episode, Boolean)]

    val epCount = episodes.length
    val session = driver.session

    val transaction = session.beginTransaction()//TODO: transactionConfig)
    if(update){
      //TODO: update
      episodes.map((_, false))
    }else{
      try {
        results = episodes.map(ep => {
          val script =
            s"MERGE (ep: Episode {name: '${ep.name}', dateTime: '${ep.datetime.toString}', nr: ${ep.episode}, url: '${ep.url}'})" ++
              s" ON MATCH SET ep.log = 'match'" ++
              s" ON CREATE SET ep.log = 'create'" ++
              s" ON CREATE SET ep.creationTimestamp = datetime('${LocalDateTime.now(ZoneId.of("UTC+2")).toString}')" ++
            s" MERGE (series: Series {seriesId: ${ep.seriesId}})" ++
              s" ON MATCH SET series.log = 'match'" ++
              s" ON CREATE SET series.log = 'create'" ++
              s" ON CREATE SET series.creationTimestamp = datetime('${LocalDateTime.now(ZoneId.of("UTC+2")).toString}')"

          val fullScript = ep.season match {
            case Some(s) => script ++ s" MERGE (season: Season {nr: $s, seriesId: ${ep.seriesId}})" ++
              s" ON MATCH SET season.log = 'match'" ++
              s" ON CREATE SET season.log = 'create'" ++
              s" ON CREATE SET season.creationTimestamp = datetime('${LocalDateTime.now(ZoneId.of("UTC+2")).toString}')" ++
              s" MERGE (ep)-[seasonRel:BELONGS_TO_SEASON]->(season)" ++
              s" ON MATCH SET seasonRel.log = 'match'" ++
              s" ON CREATE SET seasonRel.log = 'create'" ++
              s" ON CREATE SET seasonRel.creationTimestamp = datetime('${LocalDateTime.now(ZoneId.of("UTC+2")).toString}')" ++
              s" MERGE (season)-[seriesRel:BELONGS_TO_SERIES]->(series)" ++
              s" ON MATCH SET seriesRel.log = 'match'" ++
              s" ON CREATE SET seriesRel.log = 'create'" ++
              s" ON CREATE SET seriesRel.creationTimestamp = datetime('${LocalDateTime.now(ZoneId.of("UTC+2")).toString}')" ++
              s" RETURN ep.log, season.log, series.log, seasonRel.log, seriesRel.log"
            case None => script ++ s"MERGE (season: Season {nr: 0, seriesId: ${ep.seriesId}})" ++
            s" ON MATCH SET season.log = 'match" ++
            s" ON CREATE SET season.log = 'create'" ++
            s" ON CREATE SET season.creationTimestamp = datetime('${LocalDateTime.now(ZoneId.of("UTC+2")).toString}')" ++
            s" MERGE (ep)-[seasonRel:BELONGS_TO_SEASON]->(season)" ++
            s" ON MATCH SET seasonRel.log = 'match'" ++
            s" ON CREATE SET seasonRel.log = 'create'" ++
            s" ON CREATE SET seasonRel.creationTimestamp = datetime('${LocalDateTime.now(ZoneId.of("UTC+2")).toString}')" ++
            s" MERGE (season)-[seriesRel:BELONGS_TO_SERIES]->(series)" ++
            s" ON MATCH SET seriesRel.log = 'match'" ++
            s" ON CREATE SET seriesRel.log = 'create'" ++
            s" ON CREATE SET seriesRel.creationTimestamp = datetime('${LocalDateTime.now(ZoneId.of("UTC+2")).toString}')" ++
            s" RETURN ep.log, season.log, series.log, seasonRel.log, seriesRel.log"
          }

          //LOGGER.info(s"[Mediator] Running Script:  $fullScript")
          val result = transaction.run(fullScript)
          val res = result.list(rec => rec.fields().asScala.toList.map(pair => (pair.key(), pair.value().asString()))).asScala.toList.flatten
          val success = !res.exists(kv => kv._2 == "fail")

          (ep,success)

        })
        if(!results.contains(false)) {
          transaction.commit()
          LOGGER.info(s"[Mediator] Transaction committed for $epCount episodes")
        }else{
          LOGGER.error(s"[Mediator] Encountered errors. Changes will be rolled back.")
        }

        results
      }
      catch {
        case exception: Exception => {
          LOGGER.error(s"[Mediator] Encountered errors. Changes will be rolled back. Error: ", exception)
          transaction.rollback()
          val results = episodes.map(ep => (ep, false))
          results
        }
      }finally{
        transaction.close()
        session.close()
        println("closed session/transaction")
        LOGGER.info("Transaction and session closed")
      }






    }
  }
  private def checkResult(result: DbResult):Boolean = {
    result match{
      case AddResult(epLog, seasonLog, seriesLog, seasonRel, seriesRel) => !(epLog.equals("fail") || seasonLog.equals("fail") || seriesLog.equals("fail") || seasonRel.equals("fail") || seriesRel.equals("fail"))
    }
  }

  def search(query: SearchQuery): List[Episode] = {
    query match {
      case EpisodeNLQ(txt) => {
        LOGGER.info(s"Executing EpisodeNLQ with text: ${txt} ")
        val search = s"CALL db.index.fulltext.queryNodes(\"episodeNames\", \"${txt}\") YIELD node, score" ++
          s" MATCH (node)-[:BELONGS_TO_SEASON]->(s)-[:BELONGS_TO_SERIES]->(series)" ++
          s" RETURN node.name,score,node.nr,s.nr,series.seriesId,node.url"
        val session = driver.session
        val transaction = session.beginTransaction()
        val res = transaction.run(search)
        val records = res.list().asScala.toList

        val debug = records.map(record => record.fields().asScala.toList.map(pair => (pair.key(), pair.value().toString))).map(_.toMap)
        val episodes = debug.map(nameAndScore => Episode(nameAndScore.get("s.nr").flatMap(_.toIntOption), nameAndScore.getOrElse("node.nr", "0").toInt, nameAndScore.getOrElse("node.name", "--[NO NAME]--"), nameAndScore.getOrElse("series.seriesId", "0").toInt, LocalDateTime.now(), nameAndScore.getOrElse("node.url", "")))
        println("Result of " + episodes.length + " returned")
        //TODO parse result
        transaction.close()
        session.close()
        return episodes//List.empty[Episode]
      }
      case EpisodeQuery(txtOption, tags, seriesId, episode) =>
          LOGGER.info(s"Executing EpisodeQuery with params: \n\ttxt = ${txtOption.getOrElse("")}\n\ttags = {${tags.toList.map(t => s"\t\t${t._1}: [${t._2.mkString(",")}]\n")}}")
          txtOption match {
              case None => {
                  
              }
              case Some(txt) =>
          }
      case query => {
        LOGGER.error(s"Search not implemented for ${query.getClass.toString}")
        return List.empty[Episode]
      }
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