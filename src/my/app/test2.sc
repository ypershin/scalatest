package examples

object test2 {

  // val lst = List(("A", 1,1),("A",1,2),("B",2,3),("B",2,4))

  val lst = List(("A", 1), ("A", 1), ("B", 2), ("B", 2))
                                                  //> lst  : Array[(String, Int)] = Array((A,1), (A,1), (B,2), (B,2))

  //  lst groupBy (p => (p._1,p._2)) map {
  //  	case(k,v) => (p._1,p._2, (v map (_._3) sum))
  //  }

  //	lst groupBy (p => p._1) map {
  //		case(k,v) => (k._1, (v map (_._2) sum))
  //	}
  
  

  val in = List(("PO123", "P001", "Item 1", 10, 10), ("PO123", "P001", "Item 1", 10, 10),
    ("PO123", "P002", "Item 2", 30, 30), ("PO123", "P002", "Item 2", 10, 10))
                                                  //> in  : List[(String, String, String, Int, Int)] = List((PO123,P001,Item 1,10,
                                                  //| 10), (PO123,P001,Item 1,10,10), (PO123,P002,Item 2,30,30), (PO123,P002,Item 
                                                  //| 2,10,10))

  val lst2 = in groupBy (p => (p._1, p._2, p._3)) map {
    case (k, v) =>
      (k._1, k._2, k._3, (v map (_._4) sum), (v map (_._5) sum))
  }                                               //> lst2  : scala.collection.immutable.Iterable[(String, String, String, Int, In
                                                  //| t)] = List((PO123,P001,Item 1,20,20), (PO123,P002,Item 2,40,40))
	println(lst2.mkString("\n"))              //> (PO123,P001,Item 1,20,20)
                                                  //| (PO123,P002,Item 2,40,40)


}