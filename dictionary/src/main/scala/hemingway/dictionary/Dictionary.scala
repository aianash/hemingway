package hemingway
package dictionary

import scala.util.Sorting
import scala.collection.JavaConversions._
import scala.collection.SortedSet

import java.util.Collections
import java.io.File

import similarity.Similarity
import persistence._

import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap, ObjectRBTreeSet}
import it.unimi.dsi.fastutil.longs.{Long2LongOpenHashMap, LongRBTreeSet}

import org.mapdb._


/**
 * Dictionary, with approximate and exact matching. With persistence store (backed by filesystem).
 *
 * Based on - "Simple and Efficient Algorithm for Approximate Dictionary Matching" (N. Okazki, J. Tsujii 2010)
 *
 */
abstract class Dictionary {

  import Dictionary.Entry

  def persistence: DictionaryPersistence
  def ngramN: Int

  private implicit val scoreOrdering = new Ordering[Entry] {
    def compare(e1: Entry, e2: Entry) = {
      val c = e1.score.compare(e2.score)
      if(c == 0) e1.id.compare(e2.id)
      else c
    }
  }

  /** add a new string with its payload if it
    * does not exist.
    *
    * @param entry - String and its payload
    */
  def +=(entry: (String, Map[String, String])): Unit =
    add(entry._1, entry._2)

  /** add a new string without any provided payload
    *
    * @param str - string to add if it does not exist
    */
  def +=(str: String): Unit = add(str, Map.empty[String, String])

  /** Check whether this dictionary contains the exact string
    *
    * @param str - string to check
    * @return boolean - contains or not
    */
  def apply(str: String) = persistence.getExact(str.toLowerCase).isEmpty

  /** Alias for `+=` only that this returs the dictionary id
    * associated with the string
    *
    * @param str - string to add
    * @param payload - associated payload
    * @return id - dictionary id for the string
    */
  def add(str: String, payload: Map[String, String]) =
    persistence.getExact(str.toLowerCase) getOrElse directlyPut(str, payload)


  /** Get Payload of the string (with exact match)
    *
    * @param Parameter1 - blah blah
    * @return Return value - blah blah
    */
  def getPayload(str: String): Option[Map[String, String]] =
    persistence.getExact(str.toLowerCase).flatMap(id => persistence.resolvePayload(id))

  /** If string's dictionary id is known then get the payload
    *
    * @param id - id of string in this dictionary
    * @return payload - payload associated with the string
    */
  def getPayload(id: Long) = persistence.resolvePayload(id)

  /** Find topK similar strings to the given string. This uses
    * the approximate string dictionary matching algorigthm as
    * described in (N. Okazki, J, Tsujii 2010)
    *
    * @param target       target string for which to find similar
    * @param similarity   similarity to use for computing
    * @param topK         number of top similar strings to find
    * @return sorted set of topK similar string
    */
  def findSimilar(target: String, similarity: Similarity, topK: Int = 10) = {
    val feature = new NGram.Feature(target.toLowerCase, ngramN)
    val scorer = similarity.scorer(feature.size)
    var result = new ObjectRBTreeSet[Entry](scoreOrdering)

    for(l <- scorer.min to scorer.max) {
      val tau = scorer.min_overlap(l)
      val ret = cpmerge(feature, tau, l, scorer)
      result.addAll(ret)
    }

    var ret = SortedSet.empty[Entry](scoreOrdering)
    val itr = result.iterator
    var count = topK
    while(itr.hasNext && count > 0) {
      val n = itr.next
      persistence.resolvePayload(n.id) foreach(p => n.payload = p)
      ret = ret + n
      count -= 1
    }
    ret
  }

  private def cpmerge(feature: NGram.Feature, tau: Int, l: Int, scorer: Similarity#Scorer) = {
    val ordered = Sorting.stableSort[String, Long](feature, x => persistence.count(l, x))
    val counts = new Long2LongOpenHashMap
    counts.defaultReturnValue(0)

    for(k <- 0 to (ordered.size - tau)) {
      for(entryId <- persistence.get(l, ordered(k))) {
        counts.addTo(entryId, 1)
      }
    }

    var results = new ObjectRBTreeSet[Entry](scoreOrdering)

    for(k <- (ordered.size - tau + 1) to (ordered.size - 1)) {
      for(entryId <- counts.keySet) {
        if(persistence.get(l, ordered(k)).contains(entryId)) counts.addTo(entryId, 1)
        val count = counts.get(entryId)
        if(tau <= count) {
          val score = scorer.score(count.toInt, feature.size, l)
          val entry = new Entry(entryId, score)
          results.add(entry)
          counts.remove(entryId)
        } else if((count + ordered.size - k - 1) < tau) counts.remove(entryId)
      }
    }

    results
  }

  private def directlyPut(str: String, payload: Map[String, String]) = {
    val feature = new NGram.Feature(str.toLowerCase, ngramN)
    persistence.put(feature, payload + ("form" -> feature.str))
  }

}

object Dictionary {

  val DEFAULT_NGRAM_N = 2

  class Entry (val id: Long, val score: Double) {
    def str = payload.get("form")
    def str_= (s: String): Unit = payload += ("form" -> s)
    var payload = Map.empty[String, String]
  }

}


case class InMemoryDictionary(ngramN: Int = Dictionary.DEFAULT_NGRAM_N) extends Dictionary {
  val persistence = new InMemoryPersistence
}

/** File based dictionary. Internally it uses mapdb.
  */
class FileBasedDictionary private (db: DB) extends Dictionary {
  val atomicNgramN = db.atomicInteger("ngramN")
  atomicNgramN.compareAndSet(0, Dictionary.DEFAULT_NGRAM_N)

  val ngramN = atomicNgramN.get
  val persistence = new FileBasedPersistence(db)
}


object FileBasedDictionary {

  def apply(db: DB) = new FileBasedDictionary(db)

  def apply(dbPath: String) = new FileBasedDictionary(mkDB(dbPath))

  private def mkDB(dbPath: String) =
    DBMaker.fileDB(new File(dbPath))
          .asyncWriteFlushDelay(1)
          .cacheHardRefEnable
          .transactionDisable
          .closeOnJvmShutdown
          .compressionEnable
          .fileMmapEnableIfSupported
          .make
}


// object DictionaryTest {
//   def main(args: Array[String]){
//     val dic = FileBasedDictionary("alt-dataset-3")
//     println("Added " + dic.add("work", Map("pos" -> "NNP")))
//     println("Added " + dic.add("party", Map("pos" -> "NNP")))
//     println("Added " + dic.add("office", Map("pos" -> "NNP")))
//     println("Added " + dic.add("dance", Map("pos" -> "NNP")))
//     println("Added " + dic.add("date", Map("pos" -> "NNP")))

//     val sim = similarity.Jaccard(0.3)
//     dic.findSimilar("wor", sim, 1).foreach(x => println(x.id + " " + x.str))
//     dic.findSimilar("data mi", sim, 1).foreach(x => println(x.id + " " + x.str))
//     dic.findSimilar("off", sim, 1).foreach(x => println(x.id + " " + x.str))
//   }
// }