package edu.knowitall.tac2013.findSlotFillersApp

import edu.knowitall.tac2013.openie.KbpExtraction

/**
 * Finds fills given candidates for a particular slot.
 */
class SlotFillReranker(fmt: OutputFormatter) {

  import SlotFillReranker._
  
  /**
   * Requires that all candidates (if any) are for the same slot.
   */
  def findSlotAnswers(slot: Slot, kbpQuery: KBPQuery, slotCandidates: Seq[Candidate]): Seq[Candidate] = {
    
    if (slotCandidates.isEmpty) List.empty
    
    else {
      
      val maxAnswers = slot.maxResults
      
      require(slotCandidates.forall(_.pattern.slotName.equals(slot.name)))
      
      val trimGroups = mergeByLinks(slotCandidates)
      
      fmt.printFillGroups("Grouped by link then trim:", slot, trimGroups)
      
      val prefixesMerged = mergePrefixes(trimGroups)
      
      fmt.printFillGroups("Prefix trim groups merged:", slot, prefixesMerged)
      
      //val groups = getSuffixGroups(slot, prefixesMerged)
      val lastKeys = prefixesMerged.iterator.toSeq.flatMap { case (key, candidates) => candidates.map(c => (key.split(" ").last, c)) }
      val groups = lastKeys.groupBy(_._1).map { case (key, keysNcandidates) => (key, keysNcandidates.map(_._2)) }
      
      fmt.printFillGroups("Grouped by last token:", slot, groups)
      
      // rank extractions my trimFill frequency * trimFill length
      val rankedAnswers = groups.iterator.toSeq.sortBy(-_._2.size).map { case (key, candidates) =>
        val trimGroups = candidates.groupBy(_.trimmedFill.string)
        val sortedTrimGroups = trimGroups.toSeq.sortBy { case (trim, candidates) => -trim.length * candidates.size } 
        val sortedCandidates = sortedTrimGroups.flatMap { case (trim, candidates) => candidates }
        (sortedCandidates.head.trimmedFill.string, sortedCandidates)
      }
      
      fmt.printFillGroups("Ranked answer groups (descending)", slot, rankedAnswers.toMap)
      
      val bestAnswers = rankedAnswers.map(_._2.head)
      
      bestAnswers.take(maxAnswers)
    }
  }
  
  /**
   * Merge by link id, if present, else by trimmed fill string. Convert 
   * link group keys to most common trimmed fill.
   */
  def mergeByLinks(candidates: Seq[Candidate]): Map[String, Seq[Candidate]] = {
    def key(cand: Candidate) = cand.fillField.wikiLink match {
      case Some(wikiLink) => "%%" + wikiLink.fbid // use %% later so we know which ones were linked
      case None => cand.trimmedFill.string
    }
    // group by key...
    val groups = candidates.groupBy(key)
    // convert fbid keys to most common trim fill, which might make them collide with other keys,
    // so next step is to regroup and flatten...
    val convertedGroups = groups.iterator.toSeq.map { case (key, candidates) =>
      if (!key.startsWith("%%")) (key, candidates)
      else {
        // get most common trimmed fill 
        val bestTrim = candidates.groupBy(_.trimmedFill.string).maxBy(_._2.size)._2.head.trimmedFill.string
        (bestTrim, candidates)
      }
    }
    // regroup and flatten in case of key collision
    val result = convertedGroups.groupBy(_._1).map { case (key, candidateSeqs) => 
      val flatCandidates = candidateSeqs.flatMap { case (key, candidates) => candidates }
      (key, flatCandidates)
    }
    result
  }
  

  def isPrefixOf(key1: String, key2: String) = key1 != key2 && key2.startsWith(key1)
  def isSuffixOf(key1: String, key2: String) = key1 != key2 && key2.endsWith(key1)

  def mergePrefixes(trimGroups: Map[String, Seq[Candidate]]): Map[String, Seq[Candidate]] = {
    mergePairwise(trimGroups, isPrefixOf)
  }
  
  def mergePairwise(trimGroups: Map[String, Seq[Candidate]], pairEqTest: (String, String) => Boolean): Map[String, Seq[Candidate]] = {
    
    var mergedGroups = trimGroups
    
    // for each key in trimGroups, see if it is a substring of another.
    var changed = true
    while (changed) {
      val allKeyPairs = mergedGroups.keys.flatMap { key1 =>
        mergedGroups.keys.map(key2 => (removeStopTokens(key1), removeStopTokens(key2))) 
      }
      allKeyPairs find pairEqTest.tupled match {
        case Some((key1, key2)) => {
          val fullGroup = mergedGroups(key1) ++ mergedGroups(key2)
          // remove both prefix and string and add the new merged group under key string
          mergedGroups --= Seq(key1, key2) 
          mergedGroups += (key2 -> fullGroup)
          changed = true
        }
        case None => changed = false
      }
    }
    mergedGroups.toMap
  }
}

object SlotFillReranker {
  
  def removeStopTokens(str: String): String = {
    var result = str
    for (stop <- stopTokens) result.replaceAll(stop, "")
    result.trim
  }
  
  val orgAbbreviations = Set("corp", "inc", "co", "corporation", "incorporated")
  val stopTokens = orgAbbreviations ++ Set("mr", "mrs", "ms", "\\.")
  
}