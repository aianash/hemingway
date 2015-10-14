package hemingway
package dictionary
package persistence

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

import java.lang.{Long => JLong}
import java.util.NavigableSet

import Dictionary._

import org.mapdb._

import it.unimi.dsi.fastutil.longs.LongRBTreeSet


/**
 * File baed Persistence Store for Dictionary. Used MapDB
 * Structures
 *  - stores a string hashed map for quick lookup
 *  - ngram to entry indexes for each feature size
 *  - and count of entries for each ngram for each feature size
 *  - payloads (Maps) are stored separately
 *
 * @param db mapdb instance
 */
class FileBasedPersistence(dictionaryName: String, db: DB) extends DictionaryPersistence {

  private val INDEX_NAME_PREFIX  = dictionaryName + "_idx_"
  private val INDEX_COUNT_PREFIX = dictionaryName + "_idxcount_"
  private val PAYLOAD_COLL_NAME  = dictionaryName + "_payloads"
  private val HASH_INDEX_NAME    = dictionaryName + "_hashidx"
  private val UNIQUE_INCR_KEY    = dictionaryName + "_incr_key"

  private val records = db.getAll

  // Map of indices for each feature length
  // key = _idx_length
  // value = MultiMap using NavigableSet
  private var indices = records.filter(kv => isIndex(kv._1)).map(kv =>
    kv._1 -> kv._2.asInstanceOf[NavigableSet[Array[Object]]]
  )

  // for performance reasons
  // we also store count of elements in each feature length for each each (ngram)
  private var indexCounts = records.filter(kv => isIndexCount(kv._1)).map(kv =>
    kv._1 -> kv._2.asInstanceOf[BTreeMap[String, JLong]]
  )

  // Payload storage
  // payloads are stored in a separate map
  // because they can be generally very big
  // and also this can avoid unnecessary deserialization
  private var payloads = db.treeMapCreate(PAYLOAD_COLL_NAME).makeOrGet[Long, Map[String, String]]()

  // For fast exact string lookup
  private var hashIndex = db.treeMapCreate(HASH_INDEX_NAME).makeOrGet[Int, JLong]

  // node id incrementer
  private val incrkey = db.atomicLong(UNIQUE_INCR_KEY)

  def count(size: Int, ngram: String) =
    indexCounts.get(indexCountName(size)).flatMap(x => {
      Option(x.get(ngram)).map(_.longValue)
    }).getOrElse(0L)

  // Return id sorted list of entries
  def get(size: Int, ngram: String) = {
    val result = new LongRBTreeSet
    indices.get(indexName(size)).foreach { n =>
      Fun.filter(n, ngram).foreach(x => result.add(x(1).asInstanceOf[JLong]))
    }
    result
  }

  def getExact(str: String) = {
    val id = hashIndex.get(strHash(str))
    if(id != null) Some(id)
    else None
  }

  def put(feature: NGram.Feature, payload: Map[String, String]) = {
    val newId = incrkey.incrementAndGet
    val index = getOrCreateIndex(feature.size)
    val indexCount = getOrCreateIndexCount(feature.size)
    for(ngram <- feature){
      index.add(Array[Object](ngram, newId.asInstanceOf[Object]))
      val count : JLong = Option(indexCount.get(ngram)) getOrElse (0L)
      indexCount.put(ngram, count + 1L)
    }
    payloads.put(newId, payload)
    hashIndex.put(strHash(feature.str), newId)
    newId
  }

  private def getOrCreateIndexCount(size: Int) = {
    val name = indexCountName(size)
    indexCounts.get(name) match {
    case Some(indexCount) => indexCount
    case None =>
      val indexCount = db.treeMapCreate(name).make[String, JLong]()
      indexCounts += (name -> indexCount)
      indexCount
    }
  }

  private def getOrCreateIndex(size: Int) = {
    val idxName = indexName(size)
    indices.get(idxName) match {
      case Some(index) => index
      case None =>
        val index = db.treeSetCreate(idxName)
                      .serializer(BTreeKeySerializer.ARRAY2)
                      .make[Array[Object]]
        indices += (idxName -> index)
        index
    }
  }

  def resolvePayload(id: JLong) = Option(payloads.get(id))

  def entries = payloads.iterator.map(x => x._2("form") -> x._2)

  private def isIndex(name: String) = name.contains(INDEX_NAME_PREFIX)
  private def isIndexCount(name: String) = name.contains(INDEX_COUNT_PREFIX)

  private def indexName(size: Int) = INDEX_NAME_PREFIX + size
  private def indexCountName(size: Int) = INDEX_COUNT_PREFIX + size
  private def idxName2count(name: String) = name.replace(INDEX_NAME_PREFIX, INDEX_COUNT_PREFIX)
}