package FastPagerank

import akka.actor.{ActorRef, Actor}

/**
  * Created by zhaojie on 4/18/16.
  */
class DeltaVertex extends Actor {

  var neighbors: List[ActorRef] = List[ActorRef]()
  var pagerank = 0.0
  var outDegree = 0.0
  var receivedDeltaRankValue = 0.0
  var receivedRankValue = 0.0
  var ownDeltaRankValue = 0.0
  var firstIteration = true
  val id = self.path.name

  def receive = {

    // Initialize this vertex
    case InitializeVertex(rankValue, neighborActorRefs) =>
      pagerank = rankValue
      neighbors = neighborActorRefs
      outDegree = neighbors.length
      sender ! true

    // spread pagerank values to neighbors
    case SpreadRankValue =>
      if (outDegree == 0)
        sender ! 0.0
      else if (firstIteration) {
        val amountPerNeighbor = pagerank / outDegree
        neighbors.foreach(_ ! contributeRankValue(amountPerNeighbor))
        sender ! amountPerNeighbor
      }
      else {
        if (ownDeltaRankValue != 0) {
          val incrementalAmoutPerNeighbor = ownDeltaRankValue / outDegree
          neighbors.foreach(_ ! contributeRankValue(incrementalAmoutPerNeighbor))
          sender ! incrementalAmoutPerNeighbor
        }
        else
          sender ! 0.0
      }

    // Accumulate rank value or the delta rank value
    case contributeRankValue(contribution) =>
      if (firstIteration)
        receivedRankValue += contribution
      else
        receivedDeltaRankValue += contribution

    // return page rank value
    case GetRankValue =>
      sender ! (id, pagerank)

    // update pagerank values
    case Update(uniformJumpFactor, jumpFactor) =>
      if (firstIteration) {
        val updatedPageRank = uniformJumpFactor + (1 - jumpFactor) * receivedRankValue
        val diff = math.abs(pagerank - updatedPageRank)
        ownDeltaRankValue = diff
        pagerank = updatedPageRank
        sender ! diff
      }
      else {
        ownDeltaRankValue = (1 - jumpFactor) * receivedDeltaRankValue
        pagerank += ownDeltaRankValue
        val diff = math.abs(ownDeltaRankValue)
        sender ! diff
      }
      receivedDeltaRankValue = 0
      receivedRankValue = 0
      firstIteration = false
  }
}
