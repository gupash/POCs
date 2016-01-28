import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

/**
  * Sample Data
  * 11685147	com.apple.icdp.record./QJbyDrryf5rZdknA55pk8zf92	2015-12-27		200	RECOVER	<MacBookAir4,2> <Mac OS X;10.11.2;15C50> <com.apple.CloudServices/154.20.4>		0	com.apple.lakitu/154.20.4 CFNetwork/760.2.6 Darwin/15.2.0 (x86_64)	1451248910234	1	10
  * <MacBookAir4,2> <Mac OS X;10.11.2;15C50> <com.apple.CloudServices/154.20.4>
  * 11685147,200,RECOVER,MacBookAir4,10.11.2,15C50,Mac OS X,10,-6015/0
  **/

object EscrowproxyIcdp {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local").setAppName("EscrowProxyDF").set("spark.executor.memory", "2G").set("spark.driver.memory", "1G")
    val sc = new SparkContext(conf)

    val pattern = "[a-zA-Z]+".r

    def parseClientInfo(field: String) = {
      val fields = field.split(">")
      val devType = fields(0).trim.split(",")(0).substring(1).trim
      val osVersion = fields(1).trim.split(";")(1).trim
      val devVersion = fields(1).trim.split(";")(2).trim
      val osType = fields(1).trim.split(";")(0).substring(1).trim

      devType + "," + osVersion + "," + devVersion + "," + osType
    }

    def parseFile = {
      val fileData = sc.textFile("file:///Users/ashish/IdeaProjects/POCs/EscrowProxyDFSpark/src/main/resources/Input/escrow_data.txt")
      val filteredData = fileData.map(_.split("\\t")).filter(l => l(5).toLowerCase.matches("recover"))
      filteredData.map(l => (l(0), l(4), l(5), parseClientInfo(l(6)), l(12), l(8), l(1)))
    }

    val usefulData = parseFile
    usefulData.persist(StorageLevel.DISK_ONLY)

    if (!usefulData.isEmpty()) {

      //ICDP.RECOVER.CNT
      val recover_count = usefulData.map(_._5.toDouble).sum.toInt
      println("recover_count : " + recover_count)

      //ICDP.RECOVER.UU
      val recover_uu = usefulData.map(_._1).distinct.count
      println("recover_uu : " + recover_uu)

      val (success, failure) = usefulData.map(l => ((l._1, l._2), l._5)).partitionRDDBy(_._1._2 == "200")

      val success_cnt = success.map(_._2.toDouble).sum.toInt
      println("success_cnt : " + success_cnt)

      val success_uu = success.map(_._1._1).distinct.count.toInt
      println("success_uu : " + success_uu)

      val failure_cnt = failure.map(_._2.toDouble).sum.toInt
      println("failure_cnt : " + failure_cnt)

      val failure_uu = failure.map(_._1._1).distinct.count.toInt
      println("failure_uu : " + failure_uu)

      val pc_failure_cnt = usefulData.filter(l => (l._2 != "200" && l._6 == "-6015")).map(_._5.toDouble).sum.toInt
      println("pc_failure_cnt : " + pc_failure_cnt)

      val pc_failure_uu = usefulData.filter(l => (l._2 != "200" && l._6 == "-6015")).map(_._1).distinct.count.toInt
      println("pc_failure_uu : " + pc_failure_uu)

      val record_cnt = usefulData.filter(l => (l._7.contains("record"))).map(_._5.toDouble).sum.toInt
      println("record_cnt : " + record_cnt)

      val record_success_uu = usefulData.filter(l => (l._2 == "200" && l._7.contains("record"))).map(_._1).distinct.count.toInt
      println("record_success_uu : " + record_success_uu)

      val record_failure_uu = usefulData.filter(l => (l._2 != "200" && l._7.contains("record"))).map(_._1).distinct.count.toInt
      println("record_failure_uu : " + record_failure_uu)

      //Group KPI 1 - DVC.VER.UU
      val group_ver_uu = usefulData.map(l => (l._4.split(",")(0), l._1)).distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
      println("group_ver_uu : ")
      group_ver_uu.collect().foreach(println)

      if (!group_ver_uu.isEmpty()) {

        //Group KPI 2 - DVC.UU
        val group_uu = group_ver_uu.map(l => ((pattern findFirstIn l._1).getOrElse(None), l._2)).aggregateByKey(0)((a, v) => a + v, _+_)
        println("group_uu")
        group_uu.collect.foreach(println)

        val (macData, otherDevices) = group_uu.partitionRDDBy(_._1.toString.toLowerCase.contains("mac"))

        val mac_uu = macData.aggregate(0)((a, v) => a + v._2, _ + _)
        println("mac_uu : " + mac_uu)

      }

      val group_os_uu = usefulData.map(l => (l._4.split(",")(3), l._1)).distinct.aggregateByKey(0)((a,v) => a + 1, _+_)
      println("group_os_uu")
      group_os_uu.collect().foreach(println)

      val group_os_ver_uu = usefulData.map(l => ((l._4.split(",")(3), l._4.split(",")(1)), l._1)).distinct.aggregateByKey(0)((a,v) => a + 1, _+_)
      println("group_os_ver_uu")
      group_os_ver_uu.collect().foreach(println)

    }
  }

  implicit class RDDOps[T](rdd: RDD[T]) {
    def partitionRDDBy(f: T => Boolean): (RDD[T], RDD[T]) = {
      val passes = rdd.filter(f)
      val fails = rdd.filter(e => !f(e))
      (passes, fails)
    }
  }
}
