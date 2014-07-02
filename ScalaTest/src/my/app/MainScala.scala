package my.app

import scala.io._
import scala.collection.mutable.Map

object MainScala {

  // map hourly prices
  def loadPrice() {
    var priceMap = collection.mutable.Map[String, Double]()
    val startTime = System.currentTimeMillis()
    val fileName = "/data/fwd_price.tsv"
    var cnt = 0L

    for (line <- Source.fromFile(fileName).getLines) {
      val arr: Array[String] = line.split("\t")
      priceMap.put(arr(0).substring(0, 11) + arr(1), arr(3).toDouble)

      //      if (cnt==5) {
      //        println(md.toString)
      //      }      
      //      
      cnt += 1
    }
    println("recs:" + cnt)
    val endTime = System.currentTimeMillis()
    println("msec: " + (endTime - startTime))
  }

  /************************************************************************/

  var priceMapHr = collection.mutable.Map[String, List[Double]]()

  def loadPriceHr() {
    val startTime = System.currentTimeMillis()
    // val fileName = "/data/fwd_price_hr.tsv"
    val fileName = "C:/tmp/fwd_price_hr.tsv"

    var cnt = 0L
    for (line <- Source.fromFile(fileName).getLines) {
      val arr = line.split("\t")
      priceMapHr.put(arr(0).substring(0, 4) + ":" + arr(2).trim + ":" + arr(1).trim,
        (for (a <- arr.drop(3)) yield a.toDouble).toList)

      //      if (cnt == 2) {
      //        println(priceMapHr)
      //      }
      //      cnt += 1
    }
    val endTime = System.currentTimeMillis()
    println("msec: " + (endTime - startTime))
    // for (k <- priceMapHr.keys) if(k.startsWith("2014:07")) println(k)
  }

  /************************************************************************/

  import scala.collection.mutable.ListBuffer

  val sb = new StringBuffer()
  val lstBuff = new ListBuffer[(String, String, String, Double, Double)]

  type tp = (String, Double)

  def cf(tup: (String, String, List[Double]), year: Int, onOff: String): Unit = {

    val prc = priceMapHr.get(year + ":" + tup._2 + ":" + onOff) match {
      case Some(lst) => lst
      case None => { if (year == 2014 && tup._2.startsWith("07")) println(year + ":" + tup._2 + ":" + onOff) }; List()
    }

    if (prc != List()) {

      val tmp = (tup._1, year + "/" + tup._2.substring(0, 2) + "/01", onOff, prc.last,
        (for ((x, y) <- tup._3 zip prc) yield x * y).sum)

      lstBuff += tmp

    }

  }

  /************************************************************************/

  import java.io.FileWriter

  def calcCF: Unit = {
    val startTime = System.currentTimeMillis()
    // val fileName = "/data/site_vol.tsv"
    val fileName = "C:/tmp/site_vol.tsv"
    var cnt = 0L

    val years = List(2014, 2015, 2016, 2017, 2018, 2019, 2020)
    val onOff = List("ON", "OFF")

    for (line <- Source.fromFile(fileName).getLines) {
      val arr = line.split("\t").toList

      val tup = (arr(0), arr(1), (for (v <- arr.drop(2)) yield v.toDouble).toList)

      for (year <- years)
        for (nf <- onOff)
          cf(tup, year, nf)
    }
    val endTime = System.currentTimeMillis()

    // val fileOut = new FileWriter("/data/fileOut.tsv")
    val fileOut = new FileWriter("C:/tmp/fileOut.tsv")

    val lsG = lstBuff.toList groupBy (p => (p._1, p._2, p._3)) map {
      case (k, v) =>
        (k._1, k._2, k._3, (v map (_._4) sum), (v map (_._5) sum))
    }

    // fileOut.write(lsG.mkString("\n"))    

    for (l <- lsG) fileOut.write(l._1 + "\t" + l._2 + "\t" + l._3 + "\t" + l._4 + "\t" + l._5 + "\n")

    fileOut.close()

    println("msec: " + (endTime - startTime))
    // println(sb.toString)

  }

  /************************************************************************/

  def main(args: Array[String]) {

    // loadPrice
    loadPriceHr
    calcCF

  }

}