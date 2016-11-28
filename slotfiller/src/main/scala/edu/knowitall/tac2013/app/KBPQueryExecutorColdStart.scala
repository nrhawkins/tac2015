package edu.knowitall.tac2013.app

import java.io._
import java.nio.file.{Paths, Files}
import edu.knowitall.tac2013.solr.query.SolrQueryExecutor
import edu.knowitall.tac2013.app.FilterSolrResults.filterResults
import edu.knowitall.tac2013.app.util.DocUtils
import edu.knowitall.tac2013.solr.query.SolrHelper
import scopt.OptionParser
import edu.knowitall.collection.immutable.Interval
import scala.collection.JavaConverters._
import KBPQueryEntityType._
import edu.stanford.nlp.pipeline.Annotation
import edu.knowitall.tac2013.stanford.annotator.utils._
//import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.ling.CoreAnnotations.DocIDAnnotation
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation

object KBPQueryExecutorColdStart {

  def executeKbpQuery(queryExecutor: SolrQueryExecutor, kbpQuery: KBPQuery, outFmt: OutputFormatter, oldOrNew: String): Unit = {

    val slots = kbpQuery.slotsToFill
    
    println("slots to fill: " + slots.size)
    
    //slots.foreach(s => println("slot: " + s.name))
    
    //System.exit(0)

    val unfiltered = slots map { slot => (slot, queryExecutor.executeUnfilteredQuery(kbpQuery, slot)) } toMap
    
    //println("num unfiltered: " + unfiltered.size)
    //for(k <- unfiltered.keySet){
      ///println("num unfiltered candidates: " + k.name + " " + unfiltered(k).size)
      //unfiltered(k).foreach(c => {
        //println
        //println("doc id: " + c.extr.sentence.docId)
        //println("dedupe key: " + c.deduplicationKey)
        //println("arg1: " + c.extr.arg1.originalText + " " + c.fillField.tokenString)    
        //println("arg2: " + c.extr.arg2.originalText + " " + c.entityField.tokenString) 
      //})
    //}
    
    //println
    
    val filteredCandidates = slots map { slot => (slot, filterResults(unfiltered(slot), kbpQuery)) } toMap
    
    //println("num filtered: " + filteredCandidates.size)
    //for(k <- filteredCandidates.keySet){
      //println("num filtered candidates: " + k.name + " " + filteredCandidates(k).size)
      //filteredCandidates(k).foreach(c => {
        //println
        //println("doc id: " + c.extr.sentence.docId)
        //println("dedupe key: " + c.deduplicationKey)
        //println("arg1: " + c.extr.arg1.originalText + " " + c.fillField.tokenString)    
        //println("arg2: " + c.extr.arg2.originalText + " " + c.entityField.tokenString) 
      //})
    //}    
    
    //println
        
    DocUtils.putInTimexFormat(filteredCandidates,oldOrNew)

    val bestAnswers = slots map { slot => 
      (slot, new SlotFillReranker(outFmt).findSlotAnswers(slot, kbpQuery, filteredCandidates(slot))) 
    } toMap
    
    //println("num bestAnswers: " + bestAnswers.size)
    //for(k <- bestAnswers.keySet){
      //println("num bestAnswers candidates: " + k.name + " " + bestAnswers(k).size)
      //bestAnswers(k).foreach(c => {
        //println
        //println("doc id: " + c.extr.sentence.docId)
        //println("dedupe key: " + c.deduplicationKey)
        //println("arg1: " + c.extr.arg1.originalText + " " + c.fillField.tokenString)    
        //println("arg2: " + c.extr.arg2.originalText + " " + c.entityField.tokenString) 
      //})
    //}    

    //println
    
    val smoothedSlotBestAnswers = SlotFillConsistency.makeConsistent(bestAnswers)
   
    //println("num smoothedSlotBestAnswers: " + smoothedSlotBestAnswers.size)    
    //for(k <- smoothedSlotBestAnswers.keySet){
      //println("num ss bestAnswers candidates: " + k.name + " " + smoothedSlotBestAnswers(k).size)
      //println("slot max results: " + k.maxResults )
      //smoothedSlotBestAnswers(k).foreach(c => {
        //println
        //println("doc id: " + c.extr.sentence.docId)
        //println("dedupe key: " + c.deduplicationKey)
        //println("arg1: " + c.extr.arg1.originalText + " " + c.fillField.tokenString + " " + c.trimmedFill )    
        //println("arg2: " + c.extr.arg2.originalText + " " + c.entityField.tokenString+ " " + c.trimmedEntity ) 
      //})
    //}    

    //println
    
    outFmt.printAnswers(smoothedSlotBestAnswers, kbpQuery)
  }

  def toNameCase(name: String): String = {
    
    val nameCase = name.toLowerCase.split(" ").map(n => n.charAt(0).toUpper + n.drop(1)).mkString(" ")
    
    nameCase
  }
  
  def main(args: Array[String]) {

    var queryFile = "."
    var outputFile = "."
    var corpus = "old"
    var detailed = false
    //var detailed = true
    var corefOn = true
    var runID = "UWashington2"
    var roundID = "round1"
    //val roundID = "round2"
    //val round1QueriesFile = "queries2015_r1.xml"
    //val round2QueriesFile = "queries2015_r2.xml"
    var batchDrop = 0
    var batchDropRight = 0  
    var pathToSerializedCorpus = "."
    
    val parser = new OptionParser() {
      arg("queryFile", "Path to query file.", { s => queryFile = s })
      arg("outputFile", "Path to output file.", { s => outputFile = s })
      arg("corpus", "Either \"old\" or \"new\" or \"cs\".", { s => corpus = s })
      arg("roundID", "Either \"round1\" or \"round2\".", { s => roundID = s })
      arg("drop", "Drop first n queries.", { s => batchDrop = s.toInt })
      arg("dropRight", "Drop last n queries.", { s => batchDropRight = s.toInt })
      arg("pathToSerializedCorpus", "Drop first n queries.", { s => pathToSerializedCorpus = s })
      opt("detailed", "Produce more verbose output", { detailed = true })
      opt("coref", "Turn on coref module", { corefOn = true })
      opt("runID", "Set runID name", {s => runID = s})
    }
    
    if (!parser.parse(args)) return
    
    val queryExecutor = SolrQueryExecutor.getInstance(corpus, corefOn)
    //set configs for SolrHelper
    SolrHelper.setConfigurations(corpus, corefOn)
    
    val outputStream = new PrintStream(outputFile)
    val outputFormatter = detailed match {
      case true => OutputFormatter.detailedAnswersOnly(outputStream,runID)
      case false => OutputFormatter.formattedAnswersOnly(outputStream,runID)
    }

    println("detailed: " + detailed)
    println("corpus: " + corpus)
    println("coref: " + corefOn)
    println("round: " + roundID)
    println("drop: " + batchDrop)
    println("dropRight: " + batchDropRight)
    println("psc: " + pathToSerializedCorpus)    
    
    // -----------------------
    // deserializing testing
    // -----------------------
    
    //val fullDocNameWithPath = "/projects/WebWare6/KBP_2015/corpus/serialized/NYT_ENG_20131231.0228.ann"
    //println("fndp: " + fullDocNameWithPath)

    //val fis = new FileInputStream(fullDocNameWithPath);
    //val in = new ObjectInputStream(fis);
    //val obj = in.readObject();
    
    //return
    
    //val doc = Serializer.deserialize(fullDocNameWithPath).asInstanceOf[Annotation]
    
    //return    
    
    //val testID = doc.get(classOf[DocIDAnnotation])
    //println("doc id: " + testID)              
    //val docSentences = doc.get(classOf[SentencesAnnotation]).asScala.toList   
    //println("doc num sent: " + docSentences.size) 
    
    //return    
    
    
    val kbpQueryList = KBPQuery.parseKBPQueries(queryFile, roundID)
    //val kbpQueryList = KBPQuery.parseKBPQueries(queryFile, roundID, batchDrop, batchDropRight)
    
    println("num queries: " + kbpQueryList.size)

    //for (q <- kbpQueryList) {       
      //println("name size: " + q.name + " " + q.name.split(" ").size)
      //println("doc: " + q.doc)            
      //println("begOffset: " + q.begOffset)
      //println("endOffset: " + q.endOffset)
      //println("slot to fill: " + q.slotsToFill.toList(0).name)
      //val x = q.slotsToFill
      //val mentions = DocUtils.getCorefMentions(q.doc, Interval.closed(q.begOffset,q.endOffset))
      //val names = DocUtils.stanfordHelper.getNamesFromCorefMentions(q.doc, Interval.closed(q.begOffset,q.endOffset)).asScala

      //fullName has to have 2 or more names, has to overlap with the query name, dedupe same name, 
      //sort by number of names in the full name
      //var fullNames = names.filter(n => n.split(" ").size >= 2).
      //  filter(n => n.toLowerCase.contains(q.name.toLowerCase)). 
      //  map(n=>toNameCase(n)).toSet.toList
      //fullNames = fullNames.sortBy(f => f.split(" ").size).reverse
        //filter(n => n.contains(q.name))             
      //fullNames = fullNames.map(n=>toNameCase(n)).toSet
      //.toList.sortBy(n => n.split(" ").size).reverse
        
      //println("Number of names: " + names.size)
      //names.foreach(n => println("name: " + n))   
      //println("Number of fullNames: " + fullNames.size)
      //fullNames.foreach(n => println("fullname: " + n))      
      
      //println("Number of mentions: " + mentions.get.size)
      //val mentionList = mentions.get	    
      //println("Num mentions: " + mentionList.size)
      //val mentionsPROPER = mentionList.filter(m => m.mentionType.toString.contains("PROPER"))        
      //println("Num PROPER mentions: " + mentionsPROPER.size)
      
      //if(mentions.isDefined){
	  //  for(m <- mentionsPROPER){
	      //println(m.toString)
	  //    println(m.mentionSpan + " " + m.mentionType + " " +  m.mentionID + " " + m.gender + " " + m.headIndex + " " + m.sentNum)
      //    println(m.startIndex + " " + m.endIndex )
	  //  }
      //  val docString = SolrHelper.getRawDoc(q.doc)
        //println("doc snippet: " + doc.substring(q.begOffset-10, q.endOffset+10))
      //  println("doc: " + docString)
        
      //  val sentences  = DocUtils.stanfordHelper.getSentences(docString).asScala

      //  println("num sentences: " + sentences.size)        
        //println("first sentence: " + sentences(0))
        
      //  val tokens = DocUtils.stanfordHelper.getTokens(sentences(0)).asScala

        //val token = tokens(15)
        //println("tokens: start/head/end " + tokens(15) + " " + tokens(20) + " " + tokens(21))

        
        
        //val ners = DocUtils.stanfordHelper.getNERs(tokens.asJava).asScala

        //println("num tokens: " + tokens.size)
        //println("num ners: " + ners.size)

        //for(token <- tokens) {         
        //  println("token: " + token)          
        //}
        //for(ner <- ners) {         
        //  println("ner: " + ner)          
        //}        
        
        //println("token 20: " + tokens(19))
        //println("token 21: " + tokens(20))
        //println("third sentence: " + sentences(2))
        
        //val docBiden = SolrHelper.getRawDoc("APW_ENG_20101224.0029")
        //println("doc snippet: " + docBiden.substring(567-10, 576+10))      
        //val docBiden = SolrHelper.getRawDoc("APW_ENG_20100112.1041")
        //val docBiden = SolrHelper.getRawDoc("APW_ENG_20100112.0815")
        //this one is in the answer key, says: she died in Wilmington
        //val docBiden = SolrHelper.getRawDoc("APW_ENG_20100108.1237")
        //val docBiden = SolrHelper.getRawDoc("AFP_ENG_20100111.0136")
        //val docBiden = SolrHelper.getRawDoc("AFP_ENG_20100108.0040")
        //println("doc: " + docBiden)
        
      //} 
      
   // }

    //return

   /* var queryNameSetRound2 = Set[KBPQueryNameId]()
    var queryNameSetRound1 = Set[KBPQueryNameId]()
    
    if(roundID == "round2"){
    
      queryNameSetRound1 = KBPQuery.parseKBPQueriesToGetNamesIds(round1QueriesFile)
      queryNameSetRound2 = KBPQuery.parseKBPQueriesToGetNamesIds(round2QueriesFile)

        
      //println("queryNameSetRound1 size: " + queryNameSetRound1.size)
      //println("queryNameSetRound2 size: " + queryNameSetRound2.size)
       
    } */
    
    // ---------------------------------------------------------------------------
    // Single Name Resolver
    // ----------------------------------------------------------------------------
	  
       
	  
        // For PER queries which have a single name, replace that name with a full name,
        // if one can be determined
        for(query <- kbpQueryList){	    
          
          
	      var singleQueryNamePER = false
	      
	      try{
	      
	      singleQueryNamePER = query.entityType match{
            case PER if(query.name.split(" ").size == 1) => {
              
              val docNameWithPath = pathToSerializedCorpus + query.doc + ".ann"

              //println("snr: " + docNameWithPath)
              //Get doc from serialized corpus
              
              if (Files.exists(Paths.get(docNameWithPath))) {

                //println("snr: deserializing doc")
                
                val doc = Serializer.deserialize(docNameWithPath).asInstanceOf[Annotation]

                val (single, qname) = SingleNameResolver.singleQueryName(doc, query)
                
                /*val (single, qname) = roundID match {
                  case "round1" => SingleNameResolver.singleQueryName(doc, query)
                  case "round2" => {

     	            var queryNameSetRound2 = Set[KBPQueryNameId]()
                    var queryNameSetRound1 = Set[KBPQueryNameId]()

                    if (Files.exists(Paths.get(round1QueriesFile))) {
                      queryNameSetRound1 = KBPQuery.parseKBPQueriesToGetNamesIds(round1QueriesFile)
                    }
     	            if (Files.exists(Paths.get(round2QueriesFile))) {
                      queryNameSetRound2 = KBPQuery.parseKBPQueriesToGetNamesIds(round2QueriesFile)           
     	            }        
                    println("SNR: r1 size: " + queryNameSetRound1.size)
                    println("SNR: r2 size: " + queryNameSetRound2.size)

                    SingleNameResolver.singleQueryNameCheckQueryLists(doc, query, queryNameSetRound1, queryNameSetRound2)
      
                  }
                }*/

                // Found a full name for the single name
                if(!single) query.name = qname                    
                single
              }
              else true                  
        	}
	        case _ => false
	      }
	      
	      }catch {case e: Exception => 
	        {e.printStackTrace()
	          println("EXCEPTION: SingleNameResolver") 
	        }	  
	      } 
	      
        }
	    
    
    
    for (q <- kbpQueryList) { 

      var singleQueryNamePER = false
      
      singleQueryNamePER = q.entityType match{
            case PER if(q.name .split(" ").size == 1) => true
	        case _ => false
	  }
      
      println("sqn per: " + singleQueryNamePER)
      
      /*if(roundID == "round2"){
        
          //println("queryName: " + q.name)

          singleQueryNamePER = q.entityType match{
          case PER if(q.name.split(" ").size == 1) => {val (single, qname) = SingleNameResolver.singleQueryName(q, queryNameSetRound2, queryNameSetRound1)
        	    									   if(!single) q.name = qname 
        											   single
        											  }
	      case _ => false
	    }
      
        //println("snr: " + singleQueryNamePER + " " + q.name)  
        
      }*/
                 
      //return
      
      if(!singleQueryNamePER){      
      
      //println("kbpQuery name: " + kbpQuery.name)
      //println("kbpQuery nodeid: " + kbpQuery.nodeId)
      //println("kbpQuery numFbids: " + kbpQuery.numEntityFbids)
      //println("kbpQuery docid: " + kbpQuery.doc)
      //println("kbpQuery num docs: " + kbpQuery.docIds.size)
      //outputStream.println("Num docs: " + kbpQuery.docIds.size)
      //outputStream.println("Num docs: " + kbpQuery.docIdsAll.size)
      
      try{
        executeKbpQuery(queryExecutor, q, outputFormatter, corpus)
        //clear hashmaps from previous queries in stanford helper so we don't
        //get memory issues 
        DocUtils.stanfordHelper.clearHashMaps()
      }
      //catch any exception and print nil for every slot in the query that failed
      catch{
        case e: Exception => {
          println(e.getMessage())
          //outputFormatter.printEmpty(kbpQuery)
        }
      }
      
      }
    }
    
    outputStream.close()
  }
}
