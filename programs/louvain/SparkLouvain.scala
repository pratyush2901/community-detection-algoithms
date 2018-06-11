package home.hadoop.louvain

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.Graph.graphToGraphOps
import scala.math.BigDecimal.double2bigDecimal
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.Array.canBuildFrom





object SparkLouvain{
//Creates the louvain graph with the attribute of each vertices of the type  VertexState
def createLouvainGraph[VD: ClassTag](graph: Graph[VD,Long]) : Graph[VertexState,Long]= {
val msgs = graph.aggregateMessages[(Long)](e =>  {
e.sendToSrc((e.attr))
},
(a, b) => (a+b))
val msgs1 = graph.aggregateMessages[(Long)](
e =>  {
e.sendToDst((e.attr))
},
(a, b) => (a+b))  
val msg2=msgs1.union(msgs)
val msg3=msg2.map(a=>(a._1,a._2)).reduceByKey(_+_)
val msg4:VertexRDD[Long]=VertexRDD(msg3)
val louvainGraph = graph.outerJoinVertices(msg4)((vid,data,weightOption)=> { 
val weight = weightOption.getOrElse(0L)
val state = new VertexState()
state.community = vid
state.changed = false
state.communitySigmaTot = weight
state.internalWeight = 0L
state.nodeWeight = weight
state
}).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
return louvainGraph
}




// Creates the messages passed between each vertex to convey neighborhood community data.
def sendMsg(et:EdgeContext[VertexState,Long,Map[(Long,Long),Long]]) = {
et.sendToDst(Map((et.srcAttr.community,et.srcAttr.communitySigmaTot)->et.attr))
et.sendToSrc(Map((et.dstAttr.community,et.dstAttr.communitySigmaTot)->et.attr))    
}

//Merges neighborhood community data into a single message used by each vertex
def mergeMsg(m1:Map[(Long,Long),Long],m2:Map[(Long,Long),Long]) ={
val newMap = scala.collection.mutable.HashMap[(Long,Long),Long]()
m1.foreach({case (k,v)=>
if (newMap.contains(k)) newMap(k) = newMap(k) + v
else newMap(k) = v
})
m2.foreach({case (k,v)=>
if (newMap.contains(k)) newMap(k) = newMap(k) + v
else newMap(k) = v
})
newMap.toMap
}



//Calculates the modularity of each node
def q(currCommunityId:Long, testCommunityId:Long, testSigmaTot:Long, edgeWeightInCommunity:Long, nodeWeight:Long, internalWeight:Long, totalEdgeWeight:Long) : BigDecimal = {
val isCurrentCommunity = (currCommunityId.equals(testCommunityId));
val M = BigDecimal(totalEdgeWeight); 
val k_i_in_L =  if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity;
val k_i_in = BigDecimal(k_i_in_L);
val k_i = BigDecimal(nodeWeight + internalWeight);
val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot);
var deltaQ =  BigDecimal(0.0);
//if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
//    println("Calculates deltaq for ")
deltaQ = k_i_in - ( k_i * sigma_tot / M)
//}
//println(isCurrentCommunity+" "+deltaQ.toString)
return deltaQ;
}


//Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
// Returns a new set of vertices with the updated vertex state.
def louvainVertJoin(louvainGraph:Graph[VertexState,Long], msgRDD:VertexRDD[Map[(Long,Long),Long]], totalEdgeWeight:Broadcast[Long], even:Boolean) = {
louvainGraph.vertices.innerJoin(msgRDD)( (vid, vdata, msgs)=> {
var bestCommunity = vdata.community
var startingCommunityId = bestCommunity
var maxDeltaQ = BigDecimal(0.0);
var bestSigmaTot = 0L
msgs.foreach({ case( (communityId,sigmaTotal),communityEdgeWeight ) => 
val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vdata.nodeWeight, vdata.internalWeight,totalEdgeWeight.value)
//println("vid"+vid+"   communtiy: "+communityId+" sigma:"+sigmaTotal+" edgeweight:"+communityEdgeWeight+"  q:"+deltaQ)
if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))){
maxDeltaQ = deltaQ
bestCommunity = communityId
bestSigmaTot = sigmaTotal
}
})        

if ( vdata.community != bestCommunity && ( (even && vdata.community > bestCommunity)  || (!even && vdata.community < bestCommunity)  )  ){
//    println("\neven "+even+"  "+vid+" SWITCHED from "+vdata.community+" to "+bestCommunity)
vdata.community = bestCommunity
vdata.communitySigmaTot = bestSigmaTot  
vdata.changed = true
}
else{
        //println("\neven "+even+"  "+vid+" not SWITCHED from "+vdata.community+" to "+bestCommunity)
vdata.changed = false
}   
vdata
})
}

//The main function that handles the phase 1 of the louvain  algorithm
def louvain(sc:SparkContext, graph:Graph[VertexState,Long], minProgress:Int=1,progressCounter:Int=1) : (Double,Graph[VertexState,Long],Int)= {
    
    var louvainGraph = graph.cache()
    val graphWeight = louvainGraph.vertices.values.map(vdata=> vdata.internalWeight+vdata.nodeWeight).reduce(_+_)
    var totalGraphWeight = sc.broadcast(graphWeight) 
    //println("totalEdgeWeight: "+totalGraphWeight.value)
    
    val newMap = scala.collection.mutable.HashMap[(Long,Long),Long]()

    var msgRDD=louvainGraph.aggregateMessages(sendMsg,mergeMsg)
    // gather community information from each vertex's local neighborhood
    var activeMessages = msgRDD.count() //materializes the msgRDD and caches it in memory
     
    var minProgress= 1 
    var progressCounter:Int=1
    var updated = 0L - minProgress
    var even = false  
    var count = 0
    val maxIter = 100000 
    var stop = 0
    var updatedLastPhase = 0L
do { 
count += 1
even = ! even	   
val labeledVerts = louvainVertJoin(louvainGraph,msgRDD,totalGraphWeight,even).cache()   
val communtiyUpdate = labeledVerts.map( {case (vid,vdata) => (vdata.community,vdata.nodeWeight+vdata.internalWeight)}).reduceByKey(_+_).cache()
val communityMapping = labeledVerts.map( {case (vid,vdata) => (vdata.community,vid)}).join(communtiyUpdate).map({case (community,(vid,sigmaTot)) => (vid,(community,sigmaTot)) }).cache()

val updatedVerts = labeledVerts.join(communityMapping).map({ case (vid,(vdata,communityTuple) ) => 
vdata.community = communityTuple._1  
vdata.communitySigmaTot = communityTuple._2
(vid,vdata)
}).cache()
updatedVerts.count()
labeledVerts.unpersist(blocking = false)
communtiyUpdate.unpersist(blocking=false)
communityMapping.unpersist(blocking=false)
val prevG = louvainGraph
louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
louvainGraph.cache()
val oldMsgs = msgRDD
msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg).cache()
activeMessages = msgRDD.count()  // materializes the graph by forcing computation
oldMsgs.unpersist(blocking=false)
updatedVerts.unpersist(blocking=false)
prevG.unpersistVertices(blocking=false)
if (even) updated = 0
updated = updated + louvainGraph.vertices.filter(_._2.changed).count 
if (!even) {
//println("  # vertices moved: "+java.text.NumberFormat.getInstance().format(updated))
if (updated >= updatedLastPhase - minProgress) stop += 1
updatedLastPhase = updated
}
} while ( stop <= progressCounter && (even || (updated > 0 && count < maxIter)))
//println("\nCompleted in "+count+" cycles")
val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid,vdata,msgs)=> {
val community = vdata.community
var k_i_in = vdata.internalWeight
var sigmaTot = vdata.communitySigmaTot.toDouble
msgs.foreach({ case( (communityId,sigmaTotal),communityEdgeWeight ) => 
if (vdata.community == communityId) k_i_in += communityEdgeWeight})
val M = totalGraphWeight.value
val k_i = vdata.nodeWeight + vdata.internalWeight
var q = (k_i_in.toDouble / M) -  ( ( sigmaTot *k_i) / math.pow(M, 2) )
//println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
if (q < 0) 0 else q
})  
val actualQ = newVerts.values.reduce(_+_)
return (actualQ,louvainGraph,count/2)
}



def louvainFromStandardGraph[VD: ClassTag](sc:SparkContext,graph:Graph[VD,Long], minProgress:Int=1,progressCounter:Int=1) : (Double,Graph[VertexState,Long],Int) = {
val louvainGraph = createLouvainGraph(graph)
return louvain(sc,louvainGraph,minProgress,progressCounter)
}

//after phase 1 of louvain is completed, the below function would create the new graph with nodes as the representative of the various 
//communities formed during phase 1



def compressGraph(graph1:Graph[VertexState,Long],debug:Boolean=true) : Graph[VertexState,Long] = {
    //graph1.vertices.collect.foreach(println)
val internalEdgeWeights = graph1.triplets.flatMap(et=>{
if (et.srcAttr.community == et.dstAttr.community){
//println(" equal src:"+et.srcId+" dest:"+et.dstId+" et.srcAttr.community: "+et.srcAttr.community)
Iterator( ( et.srcAttr.community, 2*et.attr) )  // count the weight from both nodes  // count the weight from both nodes
} 
else 
    {
        //println("src:"+et.srcId+" dest:"+et.dstId+" et.srcAttr.community: "+et.srcAttr.community)
        Iterator.empty}  
}).reduceByKey(_+_)
var internalWeights = graph1.vertices.values.map(vdata=> (vdata.community,vdata.internalWeight)).reduceByKey(_+_)
val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({case (vid,(weight1,weight2Option)) =>
val weight2 = weight2Option.getOrElse(0L)
val state = new VertexState()
state.community = vid
state.changed = false
state.communitySigmaTot = 0L
state.internalWeight = weight1+weight2
state.nodeWeight = 0L
(vid,state)
}).cache()
val edges = graph1.triplets.flatMap(et=> {
val src = math.min(et.srcAttr.community,et.dstAttr.community)
val dst = math.max(et.srcAttr.community,et.dstAttr.community)
//println("src"+et.srcId+"dst "+et.dstId)
if (src != dst) Iterator(new Edge(src, dst, et.attr))
else {//println("not src"+et.srcId+"dst "+et.dstId)
Iterator.empty}
}).cache()
val compressedGraph = Graph(newVerts,edges).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
//compressedGraph.vertices.collect.foreach(println)
var nodeWeightMapFunc = (e:EdgeContext[VertexState,Long,Long]) => {e.sendToSrc(e.attr)
e.sendToDst(e.attr)}
val nodeWeightReduceFunc = (e1:Long,e2:Long) => e1+e2
val nodeWeights = compressedGraph.aggregateMessages(nodeWeightMapFunc,nodeWeightReduceFunc)
val louvainGraph1 = compressedGraph.outerJoinVertices(nodeWeights)((vid,data,weightOption)=> { 
val weight = weightOption.getOrElse(0L)
//println("vid "+vid+ "weighht "+weight+ "i.w "+data.internalWeight)
data.communitySigmaTot = weight +data.internalWeight
data.nodeWeight = weight
data
}).cache()
louvainGraph1.vertices.count()
louvainGraph1.triplets.count() // materialize the graph
newVerts.unpersist(blocking=false)
edges.unpersist(blocking=false)
return louvainGraph1
}



/*def printlouvain(graph:Graph[VertexState,Long]) = {
//print("\ncommunity label snapshot\n(vid,community,sigmaTot)\n")
graph.vertices.mapValues((vid,vdata)=> (vdata.community,vdata.communitySigmaTot)).collect().foreach(f=>println(" "+f))
}*/



/*def printedgetriplets(graph:Graph[VertexState,Long]) = {
//print("\ncommunity label snapshot FROM TRIPLETS\n(vid,community,sigmaTot)\n")
(graph.triplets.flatMap(e=> Iterator((e.srcId,e.srcAttr.community,e.srcAttr.communitySigmaTot), (e.dstId,e.dstAttr.community,e.dstAttr.communitySigmaTot))).collect()).foreach(f=>println(" "+f))
}*/
var qValues = Array[(Int,Double)]()   


def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
//graph.vertices.coalesce(1,true).saveAsTextFile("louvainOutput3"+"/level_"+level+"_vertices")
//graph.edges.coalesce(1,true).saveAsTextFile("louvainOutput3"+"/level_"+level+"_edges")
qValues = qValues :+ ((level,q))
//println(s"qValue: $q")
//sc.parallelize(qValues, 1).coalesce(1,true).saveAsTextFile("louvainOutput3"+"/qvalues"+level)
}


//************************************************ MAIN **************************************************************

def main(args:Array[String]){
val conf=new SparkConf().setAppName("louvain")
val sc=new SparkContext(conf)
var edgefile=args(1)

val inputHashFunc = (id:String) => id.toLong
var minProgress=1
var progressCounter=1
var edgeRDD = sc.textFile(edgefile,args(0).toInt).map(row=> {
val tokens = row.split(" ").map(_.trim())
tokens.length match {
case 2 => {new Edge(inputHashFunc(tokens(0)),inputHashFunc(tokens(1)),1L) }
}
})
val graph = Graph.fromEdges(edgeRDD, None)
run(sc,graph)
}



def finalSave(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
    
}





def run[VD: ClassTag](sc:SparkContext,graph:Graph[VD,Long]) = {
var louvainGraph = createLouvainGraph(graph)
var level = -1  // number of times the graph has been compressed
var q1 = -1.0    // current modularity value
var halt = false

var minProgress=1
var progressCounter=1


do {
level += 1
//println(s"\nStarting Louvain level $level")
val (currentQ,currentGraph,passes) = louvain(sc, louvainGraph,minProgress,progressCounter)
louvainGraph.unpersistVertices(blocking=false)
louvainGraph=currentGraph
saveLevel(sc,level,currentQ,louvainGraph)
if (passes > 2 && currentQ > q1 + 0.001 ){ 
q1 = currentQ
louvainGraph = compressGraph(louvainGraph)
//louvainGraph.vertices.collect.foreach(println)
}
else {
halt = true
}
}while ( !halt )
    //louvainGraph.vertices.collect.foreach(println)
finalSave(sc,level,q1,louvainGraph)  
}

}
