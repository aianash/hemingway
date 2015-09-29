package hemingway
package dictionary
package similarity


case class Jaccard(alpha: Double) extends Similarity {

  def scorer(qsize: Int) = JaccardScorer(qsize)

  case class JaccardScorer(qsize: Int) extends Scorer {
    val min = Math.ceil(alpha * qsize).toInt
    val max = Math.floor(qsize / alpha).toInt
    def min_overlap(rsize: Int) = Math.ceil(alpha * (qsize + rsize) / (1 + alpha)).toInt
    def score(xAndyC: Int, xC: Int, yC:Int): Double = {
      val xOryC = xC + yC - xAndyC
      xAndyC.toDouble / xOryC
    }
  }
}