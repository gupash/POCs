import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

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

object EscrowWithCaseClass {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local").setAppName("EscrowProxyWithCaseClass")
    val sc = new SparkContext(conf)

    //val pattern = "[a-zA-Z]+".r

   /* def parseClientInfo(field: String) = {
      val fields = field.split(">")

      val devType = fields(0).trim.split(",")(0).substring(1).trim
      val devVersion = fields(1).trim.split(";")(2).trim

      val osType = fields(1).trim.split(";")(0).substring(1).trim
      val osVersion = fields(1).trim.split(";")(1).trim

      (devType, devVersion, osType, osVersion)
    }
*/
    def parseFile(line: String) = {
      var pieces = line.split("\\t")
    }

    val fileData = sc.textFile("file:///Users/ashish/IdeaProjects/POCs/EscrowProxyDFSpark/src/main/resources/Input/escrow_data.txt")
    fileData.filter(_.contains("Ashish")).map(l => println("Ashish"))
  }
}
