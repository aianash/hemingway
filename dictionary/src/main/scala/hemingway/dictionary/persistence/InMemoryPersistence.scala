package hemingway
package dictionary
package persistence

import scala.collection.JavaConversions._

import java.lang.{Long => JLong}
import java.util.concurrent.atomic.AtomicLong

import it.unimi.dsi.fastutil.longs.LongRBTreeSet
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap


/**
 * In memory persistence store for dictionary
 *
 * [NOTE] - not thread safe
 */
class InMemoryPersistence extends DictionaryPersistence {

  private val hashIndex = new Int2LongOpenHashMap
  hashIndex.defaultReturnValue(-1L)
  private var indices = Map.empty[Int, Map[String, LongRBTreeSet]]
  private var payloads = Map.empty[Long, Map[String, String]]

  private var incrKey = new AtomicLong(0L)

  def count(size: Int, ngram: String) =
    indices.get(size).flatMap(_.get(ngram)).map(_.size.longValue).getOrElse(0L)

  def get(size: Int, ngram: String) =
    indices.get(size).flatMap(_.get(ngram)).getOrElse(new LongRBTreeSet)

  def getExact(str: String) =
    Option(hashIndex.get(strHash(str))).filter(x => x != -1)

  def put(feature: NGram.Feature, payload: Map[String, String]) = {
    val newId = incrKey.incrementAndGet
    var index = indices.get(feature.size) getOrElse(Map.empty[String, LongRBTreeSet])
    for(ngram <- feature){
      index.get(ngram).getOrElse({
        val s = new LongRBTreeSet
        index += (ngram -> s)
        s
      }).add(newId)
    }
    indices += (feature.size -> index)
    payloads += (newId -> payload)
    hashIndex.put(strHash(feature.str), newId)
    newId
  }

  def resolvePayload(id: JLong) = payloads.get(id)

  def entries = payloads.iterator.map(x => x._2("form") -> x._2)
}