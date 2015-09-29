package hemingway
package dictionary


object NGram {
  class Feature(val str: String, n: Int = 2) extends java.util.LinkedList[String] {
    str.sliding(n).map(_.mkString).foreach(this.add(_))
  }
}