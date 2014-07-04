// org.apache.spark.rdd.RDD == Array

val pr = sc.textFile("hdfs://vbx:8020/user/yury/cfin/fwd_price_hr_100.tsv") // Array[String]

val pr1 = for(s <- pr) yield s.split('\t')  // Array[Array[String]]

val pr2 = pr.map(x => (x.split('\t')(2),x)) // Array[(String, String)]

val pr3 = pr.map(x => (x.split('\t')(2),x.split('\t'))) // Array[(String, Array[String])]


class Price(val mth: String, val onoff: String, val mmdd: String, val p00: Double, val p01: Double, val p02: Double, val p03: Double, val p04: Double, val p05: Double, val p06: Double, val p07: Double, val p08: Double, val p09: Double, val p10: Double, val p11: Double, val p12: Double, val p13: Double, val p14: Double, val p15: Double, val p16: Double, val p17: Double, val p18: Double, val p19: Double, val p20: Double, val p21: Double, val p22: Double, val p23: Double, val dly: Double)

// Array[Price]
val pr3 = pr.map(_.split('\t')).map(p => new Price(p(0),p(1),p(2),p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble, p(9).toDouble, p(10).toDouble, p(11).toDouble, p(12).toDouble, p(13).toDouble, p(14).toDouble, p(15).toDouble, p(16).toDouble, p(17).toDouble, p(18).toDouble, p(19).toDouble, p(20).toDouble, p(21).toDouble, p(22).toDouble, p(23).toDouble, p(24).toDouble,p(25).toDouble, p(26).toDouble, p(27).toDouble))


val pr4 = pr3.map(x => (x.mmdd, x)) // Array[(String, Price)]

val pr5 = pr4.map {case(k,v) => (v,k)} // Array[(Price, String)]

val pr6 = pr4.map {case(k,v) => (k, v.dly)} // org.apache.spark.rdd.RDD[(String, Double)]










val pr4 = pr.groupBy(_.split('\t')(2)) // Array[(String, Iterable[String])]





val rdd = sc.textFile("hdfs://vbx/user/yury/cfin/testset.tsv")
org.apache.spark.rdd.RDD[String] = MappedRDD[3] at textFile at <console>:12

val rdd1 = rdd.groupBy(_.split("\t")(0))

rdd1.saveAsTextFile("hdfs://vbx:8020/user/yury/cfout2")
rdd1: org.apache.spark.rdd.RDD[(String, Iterable[String])] = MappedValuesRDD[23] at groupBy at <console>:14
(B,ArrayBuffer(B	1, B	3))
(A,ArrayBuffer(A	1, A	2))

