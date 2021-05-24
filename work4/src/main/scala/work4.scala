import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

object work4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("work4")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("WARN")
    val spark:SparkSession = SparkSession.builder().getOrCreate()

//    val rdd = spark.read.text("input/employee.txt")
    val rdd = sparkContext.textFile("input/employee.txt")
//    rdd.show()
    val mapRdd = rdd.map(line =>Row(line.split(",")(0),line.split(",")(1),line.split(",")(2)))
    val line1 = new StructField("id",StringType,true)
    val line2 = new StructField("name",StringType,true)
    val line3 = new StructField("age",StringType,true)

    val table = new StructType(Array(line1,line2,line3))

    val df = spark.createDataFrame(mapRdd,table)

    df.show()

    val rows = df.rdd.collect()
    for(row <- rows){
      println("id:"+row.getAs("id").toString+","+"name:"+row.getAs("name").toString + "," + "age:"+row.getAs("age").toString)
    }

  }
}
