package Incremental

import akka.actor.{Props, ActorSystem, ActorRef}
import scala.concurrent.{Future,Await}
import akka.pattern.ask
import akka.util._
import scala.concurrent.duration._
import scala.collection.mutable

/**
 * Created by nn on 4/17/2016.
 */

// The messages between vertices
case class InitializeVertex(rankVal: Double, neighbors: List[ActorRef])
object SpreadRankValue
case class Update(uniformJumpFactor: Double, jumpFactor: Double)
case class contributeRankValue(actorId: String, rankValue: Double)
object GetRankValue

object IncrementalPagerank {
  implicit val timeout = Timeout(10 seconds) // needed for `?` below

  def main (args: Array[String]): Unit = {

    val inputGraphFile = args(0)
    val jumpFactor = if (args.length > 1) args(1).toDouble else 0.15
    val topX = if (args.length > 2) args(2).toInt else 10
    val maxIters = 50
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
    val uniformJumpFactor = jumpFactor

    val system = ActorSystem("PageRankApp")
    import system.dispatcher

    println("start initialize the node")

    // Create the vertex actors,
    // put them into a map, indexed by vertex id
    val vertexActors = graph.map {
      case (vertexId, _) =>
        (vertexId, system.actorOf(Props[IncrementalVertex], vertexId))
    }.toMap

    val vertexActorsMutable = mutable.Map[String, ActorRef]() ++= vertexActors

    for (vertexId <- allToVertices) {
      vertexActorsMutable.get(vertexId) match {
        case Some(e) => ;
        case None => vertexActorsMutable.put(vertexId, system.actorOf(Props[IncrementalVertex], vertexId))
      }
    }

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

    val ready = Await.result(allReadyFuture, 50 seconds)

    // The list of vertex actors
    val vertices = vertexActors.values.toList

    var done = false
    var currentIteration = 1

    println("start page rank computing")
    val s = System.nanoTime()

    while (!done) {

      // Tell all vertices to spread the page rank values
      val rankValueFuture: Future[Double] =
        Future.traverse(vertices) {
          v => (v ? SpreadRankValue).mapTo[Double]
        }.map(_.sum)

      val totalSpreadValue = Await.result(rankValueFuture, 50 seconds)

      // Tell all vertices to update their pagerank values
      // PR(A) = d + (1 - d) (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))
      val updateValues = Update(uniformJumpFactor, jumpFactor)

      val diffsFuture: Future[Seq[Double]] =
        Future.traverse(vertices) {
          v => (v ? updateValues).mapTo[Double]
        }

      val averageDiff = Await.result(diffsFuture.map(_.sum / numVertices), 50 seconds)

      println("Iterations == " + currentIteration + ", average diff == " + averageDiff)

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
