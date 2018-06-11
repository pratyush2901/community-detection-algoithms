import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths._
import scala.reflect.{ClassTag, classTag}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import scala.reflect.{ClassTag, classTag}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._

object EdgeBetweeness
{
		//configurations required for execution
		val conf = new SparkConf()
		.setAppName("EdgeBetweeness")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		// .set("spark.kryo.registrator", "Registrator")
		val sc = new SparkContext(conf)
		type GNMap = Map[VertexId, Double]
		type RootGNMap = Map[VertexId, GNMap]
		 def makeMap(x: (VertexId, Double)*) = Map(x: _*)
		 def makeRootMap(x: (VertexId, GNMap)*) = Map(x: _*)
		  def addMaps(gnmap1: GNMap, gnmap2: GNMap): GNMap =
		(gnmap1.keySet ++ gnmap2.keySet).map {
		k => k -> (gnmap1.getOrElse(k, 0.0) + gnmap2.getOrElse(k, 0.0))
		}.toMap
		  def replaceValuesInMap(gnmap1: GNMap, gnmap2: GNMap): GNMap =
		(gnmap1.keySet ++ gnmap2.keySet).map {
		k => k -> gnmap2.getOrElse(k, gnmap1(k))
		}.toMap
		  def mergeMapsWithAdd(rootMap1: RootGNMap, rootMap2: RootGNMap): RootGNMap =
		(rootMap1.keySet ++ rootMap2.keySet).map {
		k => k -> addMaps(rootMap1.getOrElse(k, Map()), rootMap2.getOrElse(k, Map()))
		}.toMap

		def mergeMapsWithReplace(rootMap1: RootGNMap, rootMap2: RootGNMap): RootGNMap =
		(rootMap1.keySet ++ rootMap2.keySet).map {
		k => k -> replaceValuesInMap(rootMap1.getOrElse(k, rootMap2(k)), rootMap2.getOrElse(k, Map()))
		}.toMap

		 def sumGNMap(gnmap: GNMap): Double = gnmap.values.foldLeft(0.0)(_ + _)
		def computeBetweennessGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxGroupSize: Int = Int.MaxValue): Graph[VD, Double] = {
		def betweennessGraphForRoots(roots:Seq[VertexId]): Graph[VD, Double] = {
		val shortestPaths: Graph[SPMap, ED] = ShortestPaths.run(graph, roots)
		shortestPaths.cache()

		val noEdges = shortestPaths.vertices.context.emptyRDD(classTag[Edge[VertexId]])
		var shortestPathGraph: Graph[VD, VertexId] = Graph(graph.vertices, noEdges)

		for (root <- roots) {

		val shortestPathGraphForRoot: Graph[SPMap, VertexId] = shortestPaths
		.subgraph(epred = (triplet) => {
		val srcDistance = triplet.srcAttr.getOrElse(root, Int.MaxValue)
		val dstDistance = triplet.dstAttr.getOrElse(root, Int.MaxValue)
		if ( srcDistance == Int.MaxValue) false
		else (srcDistance+1 == dstDistance)

		})
		.mapEdges(pair => root)

		val oldSPG = shortestPathGraph
		shortestPathGraph = Graph(shortestPathGraph.vertices,
		shortestPathGraph.edges ++ shortestPathGraphForRoot.edges)
		shortestPathGraph.cache()
		oldSPG.unpersistVertices(blocking=false)
		oldSPG.edges.unpersist(blocking=false)
		}
		shortestPaths.unpersistVertices(blocking=false)
		shortestPaths.edges.unpersist(blocking=false)
		// NOTE: THIS BLOWS UP THE STACK!!!!	
		val initialGraph = shortestPathGraph.mapVertices { (id, attr) =>
		if (roots.contains(id)) makeRootMap(id -> makeMap(id -> 1.0)) else makeRootMap()
		}
		val initialMessage = makeRootMap()
		println("pregel 1 start")
		val numShortestPathsGraph: Graph[RootGNMap, VertexId] = initialGraph.pregel(initialMessage, Int.MaxValue, EdgeDirection.Out)(
		(id, attr, msg) => {
		mergeMapsWithReplace(attr, msg)
		}, // Vertex Program
		triplet => {  // Send Message compute src sum
		val srcSum = sumGNMap(triplet.srcAttr.getOrElse(triplet.attr, makeMap()))
		val dstGNMap = triplet.dstAttr.getOrElse(triplet.attr, makeMap())
		val dstExpectation = dstGNMap.getOrElse(triplet.srcId, 0.0)

		if (srcSum != dstExpectation) {
		Iterator((triplet.dstId, makeRootMap(triplet.attr -> makeMap(triplet.srcId -> srcSum))))
		} else {
		Iterator.empty
		}
		},
		(a,b) => mergeMapsWithReplace(a, b) // Merge Message
		)
		// println("\n\n***********************************\n\n")
		println("pregel 1 end")
		val numShortestPathsWithEdgeWeightsGraph:Graph[RootGNMap, (VertexId, Double)] = 
		numShortestPathsGraph
		.mapTriplets(  triplet => (triplet.attr, sumGNMap(triplet.srcAttr(triplet.attr)) / sumGNMap(triplet.dstAttr(triplet.attr))))
		//for each node, create a map
		val initialGraph2 = numShortestPathsWithEdgeWeightsGraph.mapVertices { (id, attr) =>
		roots.map(root => makeRootMap(root -> makeMap(id -> 1.0)))
		.foldLeft(makeRootMap())( (acc, x) => mergeMapsWithReplace(acc, x))
		}
		// println("\n\n***********************************\n\n")
		println("pregel 2 start")
		val betweennessVertexGraph = initialGraph2.pregel(initialMessage, 10, EdgeDirection.In)(
		(id, attr, msg) => {
		mergeMapsWithReplace(attr, msg)
		},
		triplet => {  // Send Message
		val dstSum:Double = sumGNMap(triplet.dstAttr.getOrElse(triplet.attr._1, makeMap()))
		val edgeWeight:Double = triplet.attr._2
		val srcGNMap = triplet.srcAttr.getOrElse(triplet.attr._1, makeMap())
		val srcExpectation = srcGNMap.getOrElse(triplet.dstId, 0.0)

		if (dstSum*edgeWeight != srcExpectation) {
		Iterator((triplet.srcId, makeRootMap(triplet.attr._1 -> makeMap(triplet.dstId -> dstSum*edgeWeight))))
		} else {
		Iterator.empty
		}
		},
		(a,b) => mergeMapsWithReplace(a, b) // Merge Message
		)
		// println("\n\n***********************************\n\n")
		println("pregel 2 end")
		val verticesArray: Array[(VertexId, RootGNMap)] = betweennessVertexGraph.vertices.collect

		val betweennessGraphEdges = 
		betweennessVertexGraph
		.mapTriplets(triplet => triplet.attr._2 * sumGNMap(triplet.dstAttr(triplet.attr._1)))
		.edges
		Graph(graph.vertices, betweennessGraphEdges ++ betweennessGraphEdges.reverse)
		.groupEdges( (e1, e2) => e1 + e2)
		}
		var graphVertices = graph.vertices.collect.map(pair => pair._1).toSeq
		var returnGraph = graph.mapEdges(e => 0.0)
		returnGraph.cache()
		if(maxGroupSize >= graphVertices.length)
		{
		returnGraph = betweennessGraphForRoots(graphVertices)
		}
		else
		{
		while (!graphVertices.isEmpty) {
		val singleBetweennessGraph = betweennessGraphForRoots(graphVertices.take(maxGroupSize))
		val oldRG = returnGraph
		returnGraph = Graph(returnGraph.vertices,
		returnGraph.edges ++ singleBetweennessGraph.edges)
		.groupEdges( (e1, e2) => e1 + e2)
		returnGraph.cache()
		graphVertices = graphVertices.drop(maxGroupSize)
		oldRG.unpersistVertices(blocking=false)
		oldRG.edges.unpersist(blocking=false)
		}
		}
		returnGraph
		}

		
	def main(args: Array[String]) 
	{

		val inp=sc.textFile(args(0),args(1).toInt)
		val inp1=inp.map(line=>line.split(" ")).map(word=>(word(0).toLong,word(1).toLong))
		val g = Graph.fromEdgeTuples(inp1, 1)
		 val uniqueInputGraph = g.groupEdges( (e1, e2) => e1)
		  val testGraph = Graph(uniqueInputGraph.vertices, g.edges ++ g.edges.reverse)
		   var graphVertices = g.vertices.collect.map(pair => pair._1).toSeq
		   
		var h=computeBetweennessGraph(testGraph)
		h.vertices.collect.foreach(println)
	}
}