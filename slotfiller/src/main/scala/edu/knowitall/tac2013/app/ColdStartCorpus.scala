package edu.knowitall.tac2013.app

object ColdStartCorpus {
  
  private val coldStartCorpusResourcePath = "/edu/knowitall/tac2015/findSlotFillersApp/document_collection_2014.txt"
  
  private val coldStartURL = getClass().getResource(coldStartCorpusResourcePath)
  require(coldStartURL != null, "Could not find resource: " + coldStartCorpusResourcePath)

    
  val docSet =  scala.collection.mutable.Set[String]()

  // read in document list lines with latin encoding so as not to get errors.
  scala.io.Source.fromFile(coldStartURL.getPath())(scala.io.Codec.ISO8859).getLines.foreach(line => {
    
      val name = line.trim
      
      if(!docSet.contains(name)) docSet.add(name)
        
    })
    
  lazy val documents = docSet.toSet
  
}