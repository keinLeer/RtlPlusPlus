package search

import database.Mediator
import model.rtl.Episode
import org.slf4j.LoggerFactory

/**
 * 
 */
class Searchinator(mediator: Mediator) {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  lazy val cache: List[Episode] = List.empty[Episode]






}



