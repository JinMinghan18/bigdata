import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object ex4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("ex4")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val data = sc.textFile("input1",3)
//    val res = data.filter(_.trim().length>0).map(row => (row.split( " ")(0),
//    row.split(" ")(1))).partitionBy(new HashPartitioner(1)).distinct()
    val avg = data.map(line=> (line.split(" ")(0),line.split(" ")(1).toDouble
))
  .partitionBy(new HashPartitioner(1)).mapValues(x=>(x,1))
  .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>(x._1/x._2))
    avg.saveAsTextFile("output1")
    avg.collect().foreach(println)
  }
}

