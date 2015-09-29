package hemingway
package dictionary
package persistence

import scala.util.hashing.MurmurHash3

import java.lang.{Long => JLong}

import it.unimi.dsi.fastutil.longs.LongRBTreeSet

import org.mapdb.DB


trait DictionaryPersistence {
  def count(size: Int, ngram: String): Long
  def get(size: Int, ngram: String): LongRBTreeSet
  def getExact(str: String): Option[Long]
  def put(feature: NGram.Feature, payload: Map[String, String]): Long
  def resolvePayload(id: JLong): Option[Map[String, String]]

  protected def strHash(str: String) = MurmurHash3.stringHash(str, str.length)
}