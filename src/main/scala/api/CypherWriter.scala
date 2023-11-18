package api

import org.neo4j.driver.Result
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM
import org.neo4j.driver.types.Type

import scala.jdk.CollectionConverters._



object CypherWriter {

    abstract class CypherQuery(cypherQueryElements: List[CypherQueryElement]) extends CypherQueryElement(cypherQueryElements){
        override def toString: String = {
            cypherQueryElements.map(query).mkString("\n")
        }
    }

    object EmptyQuery extends CypherQuery(List.empty[CypherQueryElement])

    case class ReadQuery(cypherReadElements: List[CypherReadElement]) extends CypherQuery(cypherReadElements)

    object CypherQuery{
        def from(cypherQueryElements: List[CypherQueryElement]): Option[CypherQuery] = {
            cypherQueryElements match {
                case Nil => Some(EmptyQuery)
                case lst: List[CypherReadElement] => {
                    println("CypherQuery.from(...) is not implemented")
                    Some(ReadQuery(lst))
                }
            }
        }
    }



    abstract class CypherReadElement(cypherQueryElements: List[CypherQueryElement]) extends CypherQueryElement(cypherQueryElements)

    abstract class CypherQueryElement(cypherQueryElements: List[CypherQueryElement])

    case class MATCH(qs: List[CypherQueryElement]) extends CypherQueryElement(qs)

    //outVar specifies label of node (outVar: NODELABEL)
    case class NODE(label: Option[String], properties: PROPS, outVar: Option[String]) extends CypherReadElement(List.empty[CypherQueryElement])

    case class REL(label: Option[String], direction: DIRECTION, outVar: Option[String]) extends CypherReadElement(List.empty[CypherQueryElement]) {
        if(direction != LEFT && direction != RIGHT){
            throw new IllegalArgumentException("Invalid direction")
        }
    }

    case class RELATION(left: CypherQueryElement, right: CypherQueryElement, label: Option[String], direction: DIRECTION, outVar: Option[String]) extends CypherReadElement(List(left, right))

    abstract class DIRECTION()

    case object LEFT extends DIRECTION

    case object RIGHT extends DIRECTION

    case class PROP(outVar: Option[String], propName: Option[String]) extends CypherReadElement(List.empty[CypherQueryElement]) {
        outVar match {
            case None | Some("") => propName match {
                case None | Some("") => throw new IllegalArgumentException("Cannot create PROP with empty Strings")
                case _ => {}
            }
            case _ => {}

        }
    }



    //for RETURN clause
    case class PROPS(properties: List[KV]) extends CypherReadElement(properties)

    case class KV(key: String, value: Either[String, Int]) extends CypherReadElement(List.empty[CypherQueryElement])


    case class RETURN(props: List[PROP]) extends CypherReadElement(props)

    def query(q: CypherQueryElement): String = {
        q match{
            case NODE(label, properties, outVar) => {
                s"(${outVar.getOrElse("")}:${label.getOrElse("")} ${query(properties)})"
            }
            case RELATION(left, right, label, direction, outVar) => {
                val rel = query(REL(label, direction, outVar))
                query(left) + rel + query(right)
            }
            case REL(label, direction, outVar) => {
                val relBody = outVar.getOrElse("") + ":" + label.getOrElse("")
                direction match {
                    case LEFT => s" <-[${relBody}]- "
                    case RIGHT => s" -[${relBody}]-> "
                }
            }
            case PROP(outVar, propName) => {
                s"${outVar.getOrElse("")}.${propName.getOrElse("")}"
            }
            case RETURN(props) => {
                s"RETURN ${props.map(query).mkString(", ")}"
            }
            case MATCH(qs) => {
                s"MATCH ${qs.map(query).mkString("")}"
            }
            case PROPS(properties) => {
                properties match{
                    case Nil => ""
                    case properties: List[KV] => {
                        val props = properties.map(kv => kv.value match {
                            case Right(number) => s"${kv.key}:${number}"
                            case Left(str) => s"${kv.key}:\"${str}\""
                        }).mkString(", ")
                        s"{ ${props} }"
                    }
                }

            }
            case KV(key, value) => {
                value match {
                    case Right(number) => s"${key}:${number}"
                    case Left(str) => s"${key}:\"${str}\""
                }
            }
        }

    }
}

object CypherReader{
    object CypherResult {
        def parse(result: Result): CypherResult = {
            val records = result.list().asScala.toList
            val maps = records.map(record => record.fields().asScala.toList.map(pair => {(pair.key(), pair.value().hasType(InternalTypeSystem.TYPE_SYSTEM.INTEGER()) match{
                case true => Right(pair.value().asInt())
                case false => Left(pair.value().asString("!!!defaultString!!!"))
            })})).map(_.toMap)


            //val episodes = maps.map(nameAndScore => Episode(nameAndScore.get("s.nr").flatMap(_.toIntOption), nameAndScore.getOrElse("node.nr", "0").toInt, nameAndScore.getOrElse("node.name", "--[NO NAME]--"), nameAndScore.getOrElse("series.seriesId", "0").toInt, LocalDateTime.now()))
            println("Result returned")

            CypherResult(maps.map(CypherResultLine))
        }
    }
    case class CypherResult(lines: List[CypherResultLine])
    case class CypherResultLine(entries: Map[String, Either[String, Int]])
}