package FastPagerank

import akka.actor.{Props, ActorSystem, ActorRef}
import scala.concurrent.{Future,Await}
import akka.pattern.ask
import akka.util._
import scala.concurrent.duration._
import scala.collection.mutable

/**
  * Created by zhaojie on 4/18/16.
  */


// The messages between vertices
case class InitializeVertex(rankVal: Double, neighbors: List[ActorRef])
object SpreadRankValue
case class Update(uniformJumpFactor: Double, jumpFactor: Double)
case class contributeRankValue(rankValue: Double)
object GetRankValue

object DeltaPagerank {
  implicit val timeout = Timeout(10 seconds) // needed for `?` below

  def main (args: Array[String]): Unit = {

    val inputGraphFile = args(0)
    val jumpFactor = if (args.length > 1) args(1).toDouble else 0.15
    val topX = if (args.length > 2) args(2).toInt else 10
    val maxIters = 100
    val diffTolerance = 1E-6

    // construct the graph from the source file
    // format: a list of (from vertex id, List(to vertex id)) pair
    println("start load graph from file")
    val graph: List[(String, List[String])] =
      io.Source.fromFile(inputGraphFile).getLines.drop(4)
        .map(_.split("\t").toList)
        .map(vertices => (vertices(0), vertices(1)))
        .toList
        .groupBy(w => w._1)
        .map { case (k, v) => (k, v.map(_._2))}
        .toList

    val allToVertices =
      io.Source.fromFile(inputGraphFile).getLines.drop(4)
        .map(_.split("\t").toList)
        .map(vertices => vertices(1)).toList.distinct

    val numVertices = graph.length
    val uniformProbability = 1.0 / numVertices
    //val uniformJumpFactor = jumpFactor / numVertices
    val uniformJumpFactor = jumpFactor

    val system = ActorSystem("PageRankApp")
    import system.dispatcher

    println("start initialize the node")

    // Create the vertex actors,
    // put them into a map, indexed by vertex id
    val vertexActors = graph.map {
      case (vertexId, _) =>
        (vertexId, system.actorOf(Props[DeltaVertex], vertexId))
    }.toMap

    val vertexActorsMutable = mutable.Map[String, ActorRef]() ++= vertexActors

    for (vertexId <- allToVertices) {
      vertexActorsMutable.get(vertexId) match {
        case Some(e) => ;
        case None => vertexActorsMutable.put(vertexId, system.actorOf(Props[DeltaVertex], vertexId))
      }
    }

    // don't use this map due to this map needs to be updated and looked up frequently
    /*val vertexDiffMap = mutable.Map[String, Boolean]()
    vertexActorsMutable.keys.foreach( vertex =>
      vertexDiffMap += vertex -> true
    )*/

    // Set neighbors for each vertex
    val readyFutures: Seq[Future[Boolean]] =
      for ((vertexId, neighborList) <- graph) yield {
        val vertex = vertexActorsMutable(vertexId)
        val neighbors = neighborList.map(vertexActorsMutable)
        (vertex ? InitializeVertex(uniformProbability, neighbors)).mapTo[Boolean]
      }

    // All vertices must be ready to go
    val allReadyFuture: Future[Boolean] =
      Future.reduce(readyFutures)(_ && _)

    val ready = Await.result(allReadyFuture, 10 seconds)

    // The list of vertex actors
    val vertices = vertexActorsMutable.values.toList

    var done = false
    var currentIteration = 1

    println("start page rank computing")
    val s = System.nanoTime()

    val diffVertices = scala.collection.mutable.ListBuffer(vertices: _*)
    //val convergeDiffs = scala.collection.mutable.ListBuffer.empty[Double]

    // delta optimizations: one node only records the changes (delta) of the rank value of incoming nodes
    // if the rank value of the node keeps unchange, then don't spread its value
    // if the rank value already converge, remove it for synchronization to remove unnecessary overhead
    while (!done) {

      // Tell all vertices to spread the page rank values
     /* val diffVertices = scala.collection.mutable.ListBuffer.empty[ActorRef]
      vertexActorsMutable foreach {case (k, v) => if(vertexDiffMap.get(k) != Some(false)) diffVertices += v}*/

      val rankValueFuture: Future[Double] =
        Future.traverse(diffVertices) {
          v => (v ? SpreadRankValue).mapTo[Double]
        }.map(_.sum)

      /*val rankValueFuture: Future[Double] =
        Future.traverse(vertexActorsMutable) match {
          case (k, v) => if (vertexDiffMap.getOrElse(k, false)) (v ? SpreadRankValue).mapTo[Double]
        }*/

      val totalSpreadValue = Await.result(rankValueFuture, 10 seconds)

      // Tell all vertices to update their pagerank values
      // PR(A) = d + (1 - d) (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))
      val updateValues = Update(uniformJumpFactor, jumpFactor)

      // update for partial nodes: diffVertices or for all node: vertices
      val diffsFuture: Future[Seq[(String, Double)]] =
        Future.traverse(vertices) {
          v => (v ? updateValues).mapTo[(String, Double)]
        }

      //val averageDiff = Await.result(diffsFuture.map(_.sum / numVertices), 10 seconds)
      val vertexDiff = Await.result(diffsFuture, 10 seconds)
      var aggregateDiff = 0.0

      diffVertices.clear()

      for ((vertexId, diff) <- vertexDiff) {
        if (diff > diffTolerance) {
          //vertexDiffMap.update(vertexId, true)
          vertexActorsMutable.get(vertexId) match {
            case Some(e) => diffVertices += e
            case None => ;
          }
        }
        aggregateDiff += diff
        //else {
        //  convergeDiffs += diff
        //}
      }

      val averageDiff = aggregateDiff / numVertices
      println("Iterations == " + currentIteration + ", average diff == " + averageDiff + ", non converage nodes == " + diffVertices.length)

      currentIteration += 1

      if (currentIteration > maxIters || averageDiff < diffTolerance) {
        done = true

        // Output ten X ranked vertices
        val pagerankFutures: Future[Seq[(String, Double)]] =
          Future.traverse(vertices) {
            v => (v ? GetRankValue).mapTo[(String, Double)]
          }

        pagerankFutures.foreach {
          pageranks => {
            val topVertices = pageranks.sortBy(-_._2).take(topX)
            for ((id, rankValue) <- topVertices)
              println(id + "\t" + rankValue)
          }
        }
      }
    }

    println("time: "+(System.nanoTime - s)/1e9+"s")
    system.terminate()
  }
}
