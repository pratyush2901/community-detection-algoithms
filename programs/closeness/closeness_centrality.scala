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

object ClosenessCentrality 
{
	def main(args: Array[String]) 
	{
		//configurations required for execution
		val conf = new SparkConf()
		.setAppName("ClosenessCentrality")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		// .set("spark.kryo.registrator", "Registrator")
		val sc = new SparkContext(conf)

		//program starts from here
		val inp=sc.textFile(args(0),args(1).toInt)
		val inp1=inp.map(line=>line.split(",")).map(word=>(word(0).toLong,word(1).toLong))
		val g = Graph.fromEdgeTuples(inp1, 1)
		val uniqueInputGraph = g.groupEdges( (e1, e2) => e1)
		val testGraph = Graph(uniqueInputGraph.vertices, g.edges ++ g.edges.reverse)
		var graphVertices = g.vertices.collect.map(pair => pair._1).toSeq
		val shortestPaths = ShortestPaths.run(testGraph, graphVertices).cache()
		var sumShortestPaths=shortestPaths.mapVertices((vid,data)=>data.values.reduce(_+_))
		val numVertices=graphVertices.length
		var closenessGraph=sumShortestPaths.mapVertices((vid,data)=>(numVertices-1).toFloat/data.toFloat)
		closenessGraph.vertices.coalesce(1).saveAsTextFile("ClosenessCentralityVertices_Output/")
		closenessGraph.edges.coalesce(1).saveAsTextFile("ClosenessCentralityEdges_Output/")
	}
}