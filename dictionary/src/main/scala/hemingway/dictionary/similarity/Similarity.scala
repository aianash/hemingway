package hemingway
package dictionary
package similarity


trait Similarity {

  def alpha: Double
  def scorer(qsize: Int): Scorer

  trait Scorer {
    def min: Int
    def max: Int
    def min_overlap(rsize: Int): Int
    def score(xAndyC: Int, xC: Int, yC:Int): Double
  }

}