import akka.actor.{ActorRef, Actor}

/**
  * Created by zhaojie on 4/14/16.
  */
class Vertex extends Actor{

  var neighbors: List[ActorRef] = List[ActorRef]()
  var pagerank = 0.0
  var outDegree = 0.0
  var receivedRankValue = 0.0
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
      else {
      val amountPerNeighbor = pagerank / outDegree
      neighbors.foreach(_ ! contributeRankValue(amountPerNeighbor))
      sender ! amountPerNeighbor
    }

    // Accumulate rank value received from neighbors
    case contributeRankValue(contribution) =>
      receivedRankValue += contribution

    //return the id of the node and its current pagerank value
    case GetRankValue =>
      sender ! (id, pagerank)

    //update pagerank value based on received rank value and jump factor
    case Update(uniformJumpFactor, jumpFactor) =>
      val updatedPageRank = uniformJumpFactor + (1 - jumpFactor) * receivedRankValue
      val diff = math.abs(pagerank - updatedPageRank)
      pagerank = updatedPageRank
      receivedRankValue = 0.0
      sender ! diff

  }
}
