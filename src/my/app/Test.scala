package my.app

class Test() {
    
    def tstN(n:Int): Array[Double] = {
    val result = for(i1 <- 1 to n; i2 <- 1 to n; if i1==i2) yield 1.0/(i1+i2)
    
    var xs = Array.fill(n)(0.0) 
    result.copyToArray(xs)
    xs
  }


}