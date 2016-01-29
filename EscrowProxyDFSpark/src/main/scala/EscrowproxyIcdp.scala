import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd._

/**
  * Sample Data
  * PRS_ID	LABEL	WEB	EVENT_TYPE	EVENT_TIME	RESPONSE	SLMT	COMMAND	CLIENT_INFO	CLUBH	ERROR_CODE	USER_AGENT	PRS_ID
  * 11685147	com.apple.icdp.record./QJbyDrryf5rZdknA55pk8zf92	2015-12-27		200	RECOVER	<MacBookAir4,2> <Mac OS X;10.11.2;15C50> <com.apple.CloudServices/154.20.4>		0	com.apple.lakitu/154.20.4 CFNetwork/760.2.6 Darwin/15.2.0 (x86_64)	1451248910234	1	10
  *
  * l._1 : 1685147
  * l._2 : com.apple.icdp.record./QJbyDrryf5rZdknA55pk8zf92
  * l._3 : 2015-12-27
  * l._4 :
  * l._5 : 200
  * l._6 : RECOVER
  * l._7 : MacBookAir4
  * l._8 : 15C50
  * l._9 : Mac OS X
  * l._10 : 10.11.2
  * l._11 :
  * l._12 : 0/-6015
  * l._13 : com.apple.lakitu/154.20.4 CFNetwork/760.2.6 Darwin/15.2.0 (x86_64)
  * l._14 : 1451248910234
  * l._15 : 1
  * l._16 : 10
  *
  **/

object EscrowproxyIcdp {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local").setAppName("EscrowProxyDF").set("spark.executor.memory", "2G").set("spark.driver.memory", "1G")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val pattern = "[a-zA-Z]+".r

    def parseClientInfo(field: String) = {
      val fields = field.split(">")

      val devType = fields(0).trim.split(",")(0).substring(1).trim
      val devVersion = fields(1).trim.split(";")(2).trim

      val osType = fields(1).trim.split(";")(0).substring(1).trim
      val osVersion = fields(1).trim.split(";")(1).trim

      (devType, devVersion, osType, osVersion)
    }

    def parseFile = {
      val fileData = sc.textFile("file:///Users/ashish/IdeaProjects/POCs/EscrowProxyDFSpark/src/main/resources/Input/escrow_data.txt")
      val filteredData = fileData.map(_.split("\\t")).filter(l => l(5).toLowerCase.matches("recover"))
      filteredData.map(index =>
        flattenTuple(index(0), index(1), index(2), index(3), index(4), index(5), parseClientInfo(index(6)), index(7), index(8), index(9), index(10), index(11), index(12)))
    }

    val usefulData = parseFile
    //usefulData.take(1).foreach(println)
    usefulData.persist(StorageLevel.DISK_ONLY)


    if (!usefulData.isEmpty()) {

      /**
        * KPI : ICDP.RECOVER.CNT
        * AGG_CNT : _._16
        */
      val recover_count = usefulData.map(_._16.toDouble).sum.toInt
      println("recover_count : " + recover_count)

      /**
        * KPI : ICDP.RECOVER.UU
        * PRS_ID : _._1
        */
      val recover_uu = usefulData.map(loc => loc._1).distinct.count
      println("recover_uu : " + recover_uu)

      val (success, failure) = usefulData.map(loc => ((loc._1, loc._5), loc._16)).partitionRDDBy(_._1._2 == "200")

      /**
        * KPI : ICDP.RECOVER.SUCCESS.CNT
        * AGG_CNTs of SUCCESS : _._2
        */
      val success_cnt = success.map(loc => loc._2.toDouble).sum.toInt
      println("success_cnt : " + success_cnt)

      /**
        * KPI : ICDP.RECOVER.SUCCESS.UU
        * PRS_IDs of SUCCESS : _._1._1
        */
      val success_uu = success.map(successMap => successMap._1._1).distinct.count.toInt
      println("success_uu : " + success_uu)

      /**
        * KPI : ICDP.RECOVER.FAILURE.CNT
        * AGG_CNTs of FAILURE : _._2
        */
      val failure_cnt = failure.map(failureMap => failureMap._2.toDouble).sum.toInt
      println("failure_cnt : " + failure_cnt)

      /**
        * KPI : ICDP.RECOVER.FAILURE.UU
        * PRS_IDs of FAILURE : _._1._1
        */
      val failure_uu = failure.map(failureMap => failureMap._1._1).distinct.count.toInt
      println("failure_uu : " + failure_uu)

      /**
        * KPI : ICDP.RECOVER.FAILSUCCESS.UU
        * PRS_IDs of SUCCESS and FAILURE : _._1
        */
      val fail_success_uu = success.map(successMap => successMap._1).join(failure.map(failureMap => failureMap._1)).count
      println(fail_success_uu)

      /**
        * KPI : ICDP.RECOVER.PCFAILURE.CNT
        * RESPONSEs : l._5
        * ERROR_CODEs : l._12
        */
      val pc_failure_cnt = usefulData.filter(loc => loc._5 != "200" && loc._12 == "-6015").map(loc => loc._16.toDouble).sum.toInt
      println("pc_failure_cnt : " + pc_failure_cnt)

      /**
        * KPI : ICDP.RECOVER.PCFAILURE.UU
        * RESPONSEs : l._5
        * ERROR_CODEs : l._12
        */
      val pc_failure_uu = usefulData.filter(loc => loc._5 != "200" && loc._12 == "-6015").map(loc => loc._1).distinct.count.toInt
      println("pc_failure_uu : " + pc_failure_uu)

      /**
        * KPI : ICDP.RECOVER.RECORD.CNT
        * LABELs : l._2
        * PRS_IDs : _._16
        */
      val record_cnt = usefulData.filter(loc => loc._2.contains("record")).map(loc => loc._16.toDouble).sum.toInt
      println("record_cnt : " + record_cnt)

      /**
        * KPI : ICDP.RECOVER.RECORD.SUCCESS.UU
        * RESPONSEs : l._5
        * LABELs : l._2
        * PRS_IDs : _._1
        */
      val record_success_uu = usefulData.filter(loc => loc._5 == "200" && loc._2.contains("record")).map(loc => loc._1).distinct.count.toInt
      println("record_success_uu : " + record_success_uu)

      /**
        * KPI : ICDP.RECOVER.RECORD.FAILURE.UU
        * RESPONSEs : l._5
        * LABELs : l._2
        * PRS_IDs : _._1
        */
      val record_failure_uu = usefulData.filter(loc => loc._5 != "200" && loc._2.contains("record")).map(loc => loc._1).distinct.count.toInt
      println("record_failure_uu : " + record_failure_uu)

      /**
        * Group KPI 1 - DVC.VER.UU
        * DEVICE_TYPEs : l._7
        * PRS_IDs : l._1
        */
      val group_ver_uu = usefulData.map(loc => (loc._7, loc._1)).distinct.aggregateByKey(0)((acc, value) => acc + 1, _ + _)
      println("group_ver_uu : ")
      group_ver_uu.collect().foreach(println)

      if (!group_ver_uu.isEmpty) {

        /**
          * Group KPI 2 - DVC.UU
          * DEVICE_TYPE WITHOUT VERSION : l._1
          * COUNT OF SIMILAR DEVICES IRRESPECTIVE OF VERSIONs : l._2
          */
        val group_uu = group_ver_uu.map(loc => ((pattern findFirstIn loc._1).getOrElse(None), loc._2)).aggregateByKey(0)((acc, value) => acc + value, _ + _)
        println("group_uu")
        group_uu.collect.foreach(println)

        val (macData, otherDevices) = group_uu.partitionRDDBy(loc => loc._1.toString.toLowerCase.contains("mac"))

        /**
          * KPI : ICDP.RECOVER.DVC.MAC.UU
          */
        val mac_uu = macData.aggregate(0)((acc, value) => acc + value._2, _ + _)
        println("mac_uu : " + mac_uu)

      }

      /**
        * Group KPI 3 - OS.UU
        * OS_TYPEs : l._9
        * PRS_IDs : l._1
        */
      val group_os_uu = usefulData.map(loc => (loc._9, loc._1)).distinct.aggregateByKey(0)((acc, value) => acc + 1, _ + _)
      println("group_os_uu")
      group_os_uu.collect().foreach(println)

      /**
        * Group KPI 2 - OS.VER.UU
        * OS_TYPEs : l._9
        * OS_VERSIONS : l._10
        * PRS_IDS : l._1
        */
      val group_os_ver_uu = usefulData.map(loc => ((loc._9, loc._10), loc._1)).distinct.aggregateByKey(0)((acc, value) => acc + 1, _ + _)
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

  implicit def flattenTuple[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](t: (A, B, C, D, E, F, (G, H, I, J), K, L, M, N, O, P)): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) = {
    (t._1, t._2, t._3, t._4, t._5, t._6, t._7._1, t._7._2, t._7._3, t._7._4, t._8, t._9, t._10, t._11, t._12, t._13)
  }
}
