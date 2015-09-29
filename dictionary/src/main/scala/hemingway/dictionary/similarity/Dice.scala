package hemingway
package dictionary
package similarity


case class Dice(alpha: Double) extends Similarity {

  def scorer(qsize: Int) = DiceScorer(qsize)

  case class DiceScorer(qsize: Int) extends Scorer {
    val min = Math.ceil((alpha * qsize) / (2 - alpha)).toInt
    val max = Math.floor((qsize * (2 - alpha)) / alpha).toInt
    def min_overlap(rsize: Int) = Math.ceil(alpha * (qsize + rsize) / 2).toInt
    def score(xAndyC: Int, xC: Int, yC:Int): Double =
      (2 * xAndyC.toDouble) / (xC + yC)
  }
}