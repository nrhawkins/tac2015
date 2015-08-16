package edu.knowitall.tac2013.app

import edu.knowitall.tac2013.app.util.DocUtils
import edu.knowitall.collection.immutable.Interval
import scala.collection.JavaConverters._

object SingleNameResolver {
  

  def singleQueryName(q: KBPQuery, queryNameSetRound2: Set[String], queryNameSetRound1: Set[String]): (Boolean,String) = {

    val names = DocUtils.stanfordHelper.getNamesFromCorefMentions(q.doc, Interval.closed(q.begOffset,q.endOffset)).asScala

    //fullName has to have 2 or more names, has to overlap with the query name, dedupe same name, 
    //sort by number of names in the full name
    var fullNames = names.filter(n => n.split(" ").size >= 2).
      filter(n => n.toLowerCase.contains(q.name.toLowerCase)). 
      map(n=>toNameCase(n)).toSet.toList
    //fullNames = fullNames.sortBy(f => f.split(" ").size).reverse
      
    fullNames = fullNames.filter(n => !queryNameSetRound2.contains(n))  
    fullNames = fullNames.filter(n => !queryNameSetRound1.contains(n))
    
    val fullNameSize2 = fullNames.filter(n => n.split(" ").size == 2)
    val fullNameSize3 = fullNames.filter(n => n.split(" ").size == 3)

    // ------------------------------------------------------------------------
    // Choosing which full name to return - 
    // -----------------------------------
    //A name of size three can mean: 
    // 1) Charles Wescot Nierenberg
    // 2) Seahawk Richard Sherman
    // I have seen both of these types paired with a correct name of size 2,
    // so, here I check if there is a name of size 2 first
    // I take the name only if there is a single, because when it is not a 
    // single, the names of size 2 are of siblings or parents
    // -------------------------------------------------------------------------    
    if(fullNameSize2.size == 1 && !queryNameSetRound2.contains(fullNameSize2(0))){(false, fullNameSize2(0))}
    else if(fullNameSize3.size == 1 && !queryNameSetRound2.contains(fullNameSize3(0))){(false, fullNameSize3(0))}
    else (true, q.name) 
    
  }
  
  def toNameCase(name: String): String = {
    val nameCase = name.toLowerCase.split(" ").map(n => n.charAt(0).toUpper + n.drop(1)).mkString(" ")    
    nameCase
  }
  
}