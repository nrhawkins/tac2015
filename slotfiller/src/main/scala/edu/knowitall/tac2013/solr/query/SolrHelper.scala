package edu.knowitall.tac2013.solr.query

import jp.sf.amateras.solr.scala.SolrClient
import edu.stanford.nlp.dcoref.CorefChain.CorefMention
import edu.knowitall.collection.immutable.Interval
import edu.knowitall.tac2013.app.util.DocUtils

object SolrHelper {
  
  val solrUrlForXMLDocsFromOldCorpus = "http://knowitall:knowit!@rv-n16.cs.washington.edu:9325/solr/oldCorpus"
  val solrUrlForXMLDocsFromNewCorpus = "http://knowitall:knowit!@rv-n16.cs.washington.edu:9325/solr/newCorpus"
  val solrUrlForXMLDocsFromCSCorpus = "http://rv-n16.cs.washington.edu:8468/solr/csdocs"  
  var solrXMLDocsClient : Option[SolrClient] = None
  var solrQueryExecutor : Option[SolrQueryExecutor] = None
  
  
  def setConfigurations(oldOrNew: String, corefOn: Boolean){
    oldOrNew match{
      case "old" => {solrXMLDocsClient = Some(new SolrClient(solrUrlForXMLDocsFromOldCorpus)) }
      case "new" => {solrXMLDocsClient = Some(new SolrClient(solrUrlForXMLDocsFromNewCorpus)) }
      case "cs" => {solrXMLDocsClient = Some(new SolrClient(solrUrlForXMLDocsFromCSCorpus)) }
    }
    
    solrQueryExecutor = Some(SolrQueryExecutor.getInstance(oldOrNew, corefOn))
  }
  
  def getDocIDMapToSentNumsForEntityNameAndNodeID(entityName: String, nodeID: Option[String]) : Map[String,List[(String,Int)]] ={
 
    val client  = solrQueryExecutor.get.solrClient
    
    var solrEntityQueryString = "arg1Text:" + "\"" + entityName + "\" " + "arg2Text:" + "\"" + entityName + "\" "
    if(nodeID.isDefined){
      solrEntityQueryString += "arg1WikiLinkNodeId:" + "\"" + nodeID.get + "\" " + "arg2WikiLinkNodeId:" + "\"" + nodeID.get + "\" "
    }
    
    val docIDQuery = client.query(solrEntityQueryString)
    val result = docIDQuery.rows(1000000).getResultAsMap()
    val docSentNumPairs = for(r <- result.documents) yield {
      (r("docId").toString,r("sentNum").toString().toInt)
    }
    
    //val docList = for(r <- result.documents) yield {
    //    r("docId").toString
    //}
    //val docSet = docList.toSet
    //println("docSet size: " + docSet.size)
    
    val docIdMapListOfSentNums = docSentNumPairs.groupBy(x => x._1)
    val constrainedMap = docIdMapListOfSentNums.filter(p => (p._2.length > 2))
    val sortedMap = constrainedMap.toList.sortBy(_._2.length)(Ordering[Int].reverse).take(2000).toMap
    val filteredSortedMap = sortedMap.filter(p => (DocUtils.docLength(p._1) < 20000)).take(20)
    //little difference between take(20) and take(500)
    //val filteredSortedMap = sortedMap.filter(p => (DocUtils.docLength(p._1) < 20000)).take(500)
    filteredSortedMap
  }
  
  
  def getRawDoc(docId: String): String = {
    val query = solrXMLDocsClient.get.query("docid:\""+ docId + "\"")
    val result = query.getResultAsMap()
    if(result.documents.length != 1){
      System.err.println(docId + " was not found in corpus");
      ""
    }
    else{
      result.documents.head("xml").toString
    }
  }

}