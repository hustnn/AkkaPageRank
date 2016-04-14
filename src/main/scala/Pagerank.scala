/**
  * Created by zhaojie on 4/13/16.
  */

import akka.actor.{Props, ActorSystem, Actor, ActorRef}
import scala.concurrent.{Future,Await}
import akka.pattern.ask
import akka.util._
import scala.concurrent.duration._

object Pagerank {

  // The messages between vertices
  case class InitializeVertex(rankVal: Double, neighbors: List[ActorRef])


  def main (args: Array[String]): Unit = {

    val inputGraphFile = args(0)

    // construct the graph from the source file
    // format: a list of (from vertex id, List(to vertex id)) pair
    val graph: List[(String, List[String])] =
      io.Source.fromFile(inputGraphFile).getLines
        .map(_.split("\t").toList)
        .map(vertices => (vertices(0), vertices(1)))
        .toList
        .groupBy(w => w._1)
        .map { case (k, v) => (k, v.map(_._2))}
        .toList

    println(graph)

    val numVertices = graph.length
    val uniformProbability = 1.0 / numVertices

    val system = ActorSystem("PageRankApp")

    // Create the vertex actors,
    // put them into a map, indexed by vertex id
    val vertexActors = graph.map {
      case (vertexId, _) =>
        (vertexId, system.actorOf(Props[Vertex], vertexId))
    }.toMap

    // Set neighbors for each vertex
    val readyFutures: Seq[Future[Boolean]] =
      for ((vertexId, neighborList) <- graph) yield {
        val vertex = vertexActors(vertexId)
        val neighbors = neighborList.map(vertexActors)
        (vertex ? InitializeVertex(uniformProbability, neighbors)).mapTo[Boolean]
      }

    // All vertices must be ready to go
    val allReadyFuture: Future[Boolean] =
      Future.reduce(readyFutures)(_ && _)

    val ready = Await.result(allReadyFuture, 10 seconds)

    // The list of vertex actors
    val vertices = vertexActors.values.toList

    var done = false
    var currentIteration = 1

    while (!done) {

      // Tell all vertices to spread the page rank values
    }

  }

}
