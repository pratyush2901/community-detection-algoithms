import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import scala.reflect.{ClassTag, classTag}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._

object cc{
def main(args: Array[String]) {	

val conf = new SparkConf()
.setAppName("cc")
.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
// .set("spark.kryo.registrator", "Registrator")
val sc = new SparkContext(conf)


val inp=sc.textFile("input.txt",100)
val inp1=inp.map(line=>line.split(" ")).map(word=>(word(0).toLong,word(1).toLong))
val g = Graph.fromEdgeTuples(inp1, 1)
val triCounts = g.triangleCount().vertices
val n=g.degrees
val p=n.mapValues(x=>(x*(x-1))/2)
val res= p join triCounts
val res1=res.mapValues(x=>if (x._1!=0) (x._2.toFloat/x._1.toFloat).toFloat else -1)
res1.coalesce(1).saveAsTextFile("clustering")
}
}