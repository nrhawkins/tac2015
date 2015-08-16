package edu.knowitall.tac2013.app

import java.io._
import java.util.concurrent.atomic.AtomicInteger
import scopt.OptionParser
import scala.Option.option2Iterable

import edu.knowitall.tac2013.solr.query.SolrQueryExecutor
import edu.knowitall.tac2013.app.FilterSolrResults.filterResults
import edu.knowitall.tac2013.app.util.DocUtils
import edu.knowitall.tac2013.solr.query.SolrHelper
import edu.knowitall.tac2013.prep
import edu.knowitall.tac2013.openie._
import edu.knowitall.tac2013.prep.util.FileUtils
import edu.knowitall.tac2013.prep.util.Line
import edu.knowitall.tac2013.prep.util.LineReader
import edu.knowitall.tac2013.solr.KbpExtractionConverter
import edu.knowitall.tac2013.prep._
import edu.knowitall.tac2013.app.KBPQueryEntityType._
import edu.knowitall.tac2013.app.SemanticTaggers.getStanfordTagTypes
import edu.knowitall.tac2013.stanford.annotator.utils.StanfordAnnotatorHelperMethods
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.dcoref.CorefChain.CorefMention;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations.CorefChainAnnotation;
import collection.JavaConverters._
import edu.stanford.nlp.ling.CoreAnnotations._

object KBPExtractionsColdStart {

  private val extrCounter = new AtomicInteger(0)  
  val annotatorHelper = new StanfordAnnotatorHelperMethods()
  
  def executeKbpQuery(queryExecutor: SolrQueryExecutor, kbpQuery: KBPQuery, outFmt: OutputFormatter, oldOrNew: String): Unit = {

    val slots = kbpQuery.slotsToFill

    val unfiltered = slots map { slot => (slot, queryExecutor.executeUnfilteredQuery(kbpQuery, slot)) } toMap
    
    val filteredCandidates = slots map { slot => (slot, filterResults(unfiltered(slot), kbpQuery)) } toMap
    
    DocUtils.putInTimexFormat(filteredCandidates,oldOrNew)

    val bestAnswers = slots map { slot => 
      (slot, new SlotFillReranker(outFmt).findSlotAnswers(slot, kbpQuery, filteredCandidates(slot))) 
    } toMap
    
    val smoothedSlotBestAnswers = SlotFillConsistency.makeConsistent(bestAnswers)
    
    outFmt.printAnswers(smoothedSlotBestAnswers, kbpQuery)
  }

  def main(args: Array[String]) {

    // our input will be raw documents, not a file of extractions
    //var inputExtrs = false
    var inputPath = "."
    var outputFile = "."
    var detailed = false
    var corpus = "news"
    var runID = "UWashington_2"
      
    val parser = new OptionParser() {
      arg("inputPath", "XML or KbpExtraction input file or recursive dir.", { s => inputPath = s})
      arg("outputFile", "Path to output file.", { s => outputFile = s })      
      opt("detailed", "Produce more verbose output", { detailed = true })
      opt("corpus", "For inputRaw, specifies corpus type (news, web, forum.", { s => corpus = s })
      opt("runID", "Set runID name", {s => runID = s})
    }
    
    if (!parser.parse(args)) return
    
    //println("inputExtrs: " + inputExtrs)
    println("inputPath: " + inputPath) 
    println("outputFile: " + outputFile)
    println("detailed: " + detailed)
    println("corpus: " + corpus)
    println("runID: " + runID)
    
    val input =  {
      if (inputPath.equals("stdin")) 
        LineReader.stdin
      else {
        val files = FileUtils.getFilesRecursive(new java.io.File(inputPath))        
        val readers = files.map(f => LineReader.fromFile(f, "UTF8"))
        FileUtils.getLines(readers)
      }
    }
    
    val outputStream = new PrintStream(outputFile)
    val outputFormatter = detailed match {
      case true => OutputFormatter.detailedAnswersOnly(outputStream,runID)
      case false => OutputFormatter.formattedAnswersOnly(outputStream,runID)
    }

    //val extrs = getExtractionsFromXml(input, corpus).toSeq
    //println("num extrs: " + extrs.size)     
    //checkExtractions(extrs)
        
    val slotsToFillPER = Slot.personSlots
    val slotsToFillORG = Slot.orgSlots
    val slotsToFillAll = Slot.allSlots 
    
	//val entityTypePER = PER
	//val entityTypeORG = ORG
    //val beginOffset = 0
    //val endOffset = 0
    //val nodeId = None
    //val idText = "idText"
    //val nameText = "nameText"
    //val docIdText = "docIdText"
    
    //val perQuery = new KBPQuery(idText,nameText,docIdText,beginOffset,endOffset,entityTypePER,nodeId,slotsToFillPER)    
    //val orgQuery = new KBPQuery(idText,nameText,docIdText,beginOffset,endOffset,entityTypeORG,nodeId,slotsToFillORG)

    println("num slots to fill PER: " + slotsToFillPER.size)
    println("num slots to fill ORG: " + slotsToFillORG.size)

    //val candidates = wrapWithCandidate(extrs) 
    
    val rawDoc = "President Bill Clinton said 'hi' to Barack Obama and his wife Michelle.  President Clinton was visiting to drop off a kitty, a child of Socks."  
    val processedDoc = new Annotation(rawDoc)
    annotatorHelper.getCorefPipeline().annotate(processedDoc)
    //annotatorHelper.getCorefPipeline().annotate(processedDoc)
    
    println("rawDoc: " + rawDoc)

    val corefMap = processedDoc.get(classOf[CorefChainAnnotation]).asScala     

    val sentences = processedDoc.get(classOf[SentencesAnnotation]).asScala.toList
    
    for(sentence <- sentences){
      
      val tokens = sentence.get(classOf[TokensAnnotation]).asScala

      for(token <- tokens){
      
        println("token: " + token.originalText + " " + token.lemma + " " + token.ner)
        
      }
      
    }
    
    println("size of keySet: " + corefMap.keySet.size)
    
    for(k <- corefMap.keySet.toList.sorted){

          println("-------------------------")
          println("coref key: " + k)
          println("-------------------------")
          val corefMentions = corefMap(k).getMentionsInTextualOrder().asScala

          for(m <- corefMentions){            
            //println(m.toString())
            println("mention id: " + m.mentionID)
            println(">mention span: " + m.mentionSpan)
            println(">mention span length: " + m.mentionSpan.length())
            println(">mention type: " + m.mentionType)
            println(">mention type name(): " + m.mentionType.name())
            println(">mention sentnum: " + m.sentNum)
            println(">mention start index: " + m.startIndex)            
            println(">mention animacy: " + m.animacy)
            println(">mention gender: " + m.gender)
          }
          
        }
    
    val docSet = ColdStartCorpus.documents.toList.sorted 
    
    println("size corpus: " + docSet.size)
    println("doc: " + docSet(0))
    println("doc: " + docSet(1))
    println("doc: " + docSet(2))
    
    outputStream.close()
    
  }
  
  
  def getExtractionsFromXml(input: Iterator[Line], corpus: String): Iterator[KbpExtraction] = {
    
    // Load pipeline components
    val docProcessor = KbpDocProcessor.getProcessor(corpus)
    val sentencer = Sentencer.defaultInstance
    val parser = new KbpSentenceParser()
    val extractor = new KbpCombinedExtractor()

    //val x = new DocSplitter(input)
    //var docCount = 0    
    //while(x.hasNext){
    //  docCount += 1      
    //}
    //println("docCount: " + docCount)
    
    // Move data through the pipe in parallel.
    new DocSplitter(input).grouped(100).flatMap { docs =>
      
      val processedDocs = docs.par flatMap docProcessor.process
      
      println("procDocs size: " + processedDocs.size)
      
      val sentences = processedDocs flatMap sentencer.convertToSentences
      
      println("sentences size: " + sentences.size)
      
      val filteredSentences = sentences flatMap SentenceFilter.apply
      val parsedSentences = filteredSentences flatMap parser.parseKbpSentence
      val extractions = parsedSentences flatMap extractor.extract

      println("extractions size: " + extractions.size)     
      
      extractions
    } 
    
  }
  
  //private def wrapWithCandidate(kbpExtrs: Seq[KbpExtraction]): Seq[Candidate] = {
  //  kbpExtrs.map { extr =>
  //    new Candidate(extrCounter.getAndIncrement, kbpSolrQuery, extr, 
  //        getTagTypes(extr,kbpSolrQuery.pattern))
  //  }
  //}
  
  private def checkExtractions(extrs: Seq[KbpExtraction]): Unit = {
    
    println("checking Extractions")

    val extrsSubSet = extrs.dropRight(721212)
    
    //for(e <- extrs){
    for(e <- extrsSubSet){
      println("arg1: " + e.arg1.originalText)
      println("arg1 ti: " + e.arg1.tokenInterval)
      println("rel: " + e.rel.originalText)
      println("rel ti: " + e.rel.tokenInterval)
      println("arg2: " + e.arg2.originalText)
      println("arg2 ti: " + e.arg2.tokenInterval)
      
      val types = getStanfordTagTypes(e)

      println("types: " + types)
      
    }
    
    //extrs.grouped(100) foreach { bigGroup =>
    //  bigGroup.grouped(10).toSeq.par foreach { smallGroup =>
    //    smallGroup foreach printExtrValues
    //  }
    //}
    
  }
  
  private def printExtrValues(extr: KbpExtraction): Unit = {
    println(extrCounter.get + ": " + extr.arg1.originalText + ", " + extr.arg2.originalText + 
      ", " + extr.rel.originalText)  
  }
        
}