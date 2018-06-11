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
object DegreeCentrality 
{
	def main(args: Array[String]) 
	{
		//configurations required for execution
		val conf = new SparkConf()
		.setAppName("DegreeCentrality")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		// .set("spark.kryo.registrator", "Registrator")
		val sc = new SparkContext(conf)

		//program starts from here
		val inp=sc.textFile("input.txt")
		val inp1=inp.map(line=>line.split(" ")).map(word=>(word(0).toLong,word(1).toLong))
		val g = Graph.fromEdgeTuples(inp1, 1)
		val n=g.vertices.count
		val degree_coeff= g.degrees.mapValues(x=> (x.toFloat/(n-1).toFloat).toFloat)
		degree_coeff.coalesce(1).saveAsTextFile("DegreeCentrality_Output/")
	}
}