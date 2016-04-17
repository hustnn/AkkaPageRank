package Incremental

import akka.actor.{Actor, ActorRef}

import scala.collection.mutable

/**
 * Created by nn on 4/17/2016.
 */
class IncrementalVertex extends Actor {

  var neighbors: List[ActorRef] = List[ActorRef]()
  var pagerank = 0.0
  var outDegree = 0.0
  var receivedRankValue = 0.0
  var pageRankChanged = true
  val cachedRankValues = mutable.Map[String, Double]()
  val id = self.path.name

  def receive = {

    // Initialize this vertex
    case InitializeVertex(rankValue, neighborActorRefs) =>
      pagerank = rankValue
      neighbors = neighborActorRefs
      outDegree = neighbors.length
      sender ! true

    // spread pagerank values to neighbors
    // if its pagerank values keep unchanged, then don't need send anything
    case SpreadRankValue =>
      if (outDegree == 0 || receivedRankValue == 0)
        sender ! 0.0
      else {
        val amountPerNeighbor = pagerank / outDegree
        if (pageRankChanged)
          neighbors.foreach(_ ! contributeRankValue(id, amountPerNeighbor))
        sender ! amountPerNeighbor
      }

    // Accumulate rank value received from neighbors
    case contributeRankValue(actorId, contribution) =>
      cachedRankValues.put(actorId, contribution)

    // return current pagerank value
    case GetRankValue =>
      sender ! (id, pagerank)

    //  update pagerank value
    case Update(uniformJumpFactor, jumpFactor) =>
      val receivedRankValue = cachedRankValues.foldLeft(0)(_+_._2)
      val updatedPageRank = uniformJumpFactor + (1 - jumpFactor) * receivedRankValue
      if (updatedPageRank != pagerank) {
        pageRankChanged = true
      }
      val diff = math.abs(pagerank - updatedPageRank)
      pagerank = updatedPageRank
      sender ! diff
  }
}
