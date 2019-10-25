import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


case class PingLong(
                     date: String,
                     time: String,
                     size: Long,
                     r_version: String,
                     r_arch: String,
                     r_os:String,
                     _package: String,
                     version: String,
                     country: String,
                     ip_id: Int
                   )

object EntryPoint {
  def main(args: Array[String]): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)


    val conf: SparkConf = new SparkConf().setAppName("mySpark").setMaster("local[*]")

    val context = new SparkContext(conf)


    val initTime = System.currentTimeMillis()
    val rdd: RDD[String] = context.textFile("/mnt/important/Data/2015-12-12.csv").cache()
    val head = rdd.first() // stage -1
    val transform1 = rdd.filter(_.!=(head)).map{
      line =>
        val x  = line.split(",")
        PingLong(x(0),x(1),x(2).toLong,x(3),x(4),x(5),x(6),x(7),x(8),x(9).toInt)
    }

    println(transform1.count()) // stage -2
                                                                                 // stage -3
    val transform2 = transform1.map(pingConf => (pingConf.country,pingConf.size)).groupBy(_._1).map(x => x._1 -> x._2.map(_._2).sum)

    transform2.foreach(println) // stage - 4

    val endTime = System.nanoTime()

    println("total time execution: " + (endTime - initTime))

    println("partitions: "+rdd.getNumPartitions)



  }

}

যখন সমস্ত পার্টিশন থেকে ডাটা এগ্রিগেইট করা প্রয়োজন হয় তাকে স্টেজ বলে। যেমন: groupBy, count, first etc. কিন্ত map, filter এরা নিজ পার্টিশনেই ডাটা টার্ন্সফর্ম করে ফলে এরা স্টেজ নয়।
