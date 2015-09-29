package hemingway
package dictionary
package similarity


case class Cosine(alpha: Double) extends Similarity {

  def scorer(qsize: Int) = CosineScorer(qsize)

  case class CosineScorer(qsize: Int) extends Scorer {
    val min = Math.ceil(alpha * alpha * qsize).toInt
    val max = Math.floor(qsize / (alpha * alpha)).toInt
    def min_overlap(rsize: Int) = Math.ceil(alpha * Math.sqrt(qsize * rsize)).toInt
    def score(xAndyC: Int, xC: Int, yC:Int): Double =
      xAndyC.toDouble / Math.sqrt(xC * yC)
  }
}