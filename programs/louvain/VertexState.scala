package home.hadoop.louvain

class VertexState extends Serializable{
var community = -1L
var communitySigmaTot = 0L
var internalWeight = 0L  // self edges
var nodeWeight = 0L;  //out degree
var changed = false
override def toString(): String = {
"{community:"+community+",communitySigmaTot:"+communitySigmaTot+
",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+"}"
}
}
