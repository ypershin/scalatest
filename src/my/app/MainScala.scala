package my.app

import scala.io._

object MainScala {

  val volume = List((1000, 100, 1.0), (1000, 200, 1.1), (2000, 100, 1.5), (2000, 100, 1.6))
  val price = List((100, 10), (200, 15))

  val lst = List(1, 2, 3)

  def tst = {
    // to pivot list 
    for (l <- volume) println(l._1)
  }

  def ps = {
    var sb = new StringBuffer()
    for (v <- volume) {
      for (p <- price) {
        if (v._2 == p._1) {
          sb.append(v._1 + "\t" + v._2 + "\t" + v._3 * p._2)
        }
      }
      sb.append("\n")
    }
    sb.toString()
  }

  def readInput {
    // val fileName = "/data/fwd_price_hr.tsv"
    val fileName = "/data/stg_site_volume_hr.tsv"
    var cnt: Long = 0L
    for (line <- Source.fromFile(fileName).getLines) {
      if (line.indexOf("0702") > -1) {
        println(line)
        val arr = line.split("\t").reverse
        var b = new Array[String](arr.size-2) 
        arr.copyToArray(b)
        val ad = b.reverse.map(_.toDouble)
        println(ad.map(_+100.0).mkString(" "))
        cnt += 1
      }
      if (cnt > 2) return
    }
  }
  
  
  def testString {
    val str = "12 13"
    val lst = str.split(" ")
    println(lst.map(_*2))
   
      
  }

  def main(args: Array[String]) {
    // println(volume)
    // println(ps)
    // tst    
    // tst
    readInput
    // println(new Test().tstN(10).mkString(","))
  }

}