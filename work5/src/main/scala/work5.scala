import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object work5 {

  case class employee(name:String,sex:String,age:Int)
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("work5")
    val sparkSession:SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop101:3306/sparktest?serverTimezone=UTC")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "employee")
      .load()

    df.createTempView("employee")

    import sparkSession.implicits._

    val df2:RDD[employee] = sparkSession.sparkContext.makeRDD(List(employee("Mary","F",26),employee("Tom","M",23)))
    val ds:Dataset[employee] = df2.toDS
//    ds.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://hadoop101:3306/sparktest?serverTimezone=UTC")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("dbtable", "employee")
//      .mode(SaveMode.Append)
//      .save()

    val dffinal = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop101:3306/sparktest?serverTimezone=UTC")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "employee")
      .load()

//    dffinal.createTempView("employee2")
    dffinal.show()
    val dfquery = dffinal.agg("age"->"max","age"->"sum")
    dfquery.show()
  }

}
