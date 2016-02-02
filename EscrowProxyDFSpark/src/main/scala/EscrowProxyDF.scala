import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object EscrowProxyDF {

  val ICDP: String = "ICDP."
  val OS: String = "OS."
  val UU: String = ".UU"
  val DVC: String = "DVC."
  val SUCC_RES_CODE: String = "200"

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local").setAppName("EscroWithCase").set("spark.executor.memory", "2G").set("spark.driver.memory", "1G")
    val sc = new SparkContext(conf)
    val fileData = sc.textFile("file:///Users/ashish/IdeaProjects/POCs/EscrowProxyDFSpark/src/main/resources/Input/escrow_data.txt")

    val utility = new Utility()
    val entities = fileData.map(utility.parse)

    val filteredEntities = entities.filter(entity => entity.isValid && entity.command.matches("RECOVER"))

    filteredEntities.persist()

    val filteredOSEntities = filteredEntities.filter(entity => entity.os_valid)

    val OS_U = filteredOSEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + OS + entity.os_type + UU, entity.prs_id))
    val OS_UU = OS_U.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    OS_UU.foreach(println)

    val OS_MAJ_VER = filteredOSEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + OS + entity.os_type + "." + entity.os_major_version + UU, entity.prs_id))
    val OS_MAJ_VER_UU = OS_MAJ_VER.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    OS_MAJ_VER_UU.foreach(println)

    val OS_MAJ_MIN = filteredOSEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + OS + entity.os_type + "." + entity.os_major_version + entity.os_minor_version + UU, entity.prs_id))
    val OS_MAJ_MIN_UU = OS_MAJ_MIN.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    OS_MAJ_MIN_UU.foreach(println)


    val filteredDeviceEntities = filteredEntities.filter(entity => entity.device_valid)

    val DEVICE_TYPE = filteredDeviceEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + DVC + entity.platform_type + UU, entity.prs_id))
    val DEVICE_TYPE_UU = DEVICE_TYPE.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    DEVICE_TYPE_UU.foreach(println)

    val DEVICE_NAME = filteredDeviceEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + DVC + entity.platform_name + UU, entity.prs_id))
    val DEVICE_NAME_UU = DEVICE_NAME.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    DEVICE_NAME_UU.foreach(println)

    val filteredMacDevEntities = filteredDeviceEntities.filter(entity => !entity.platform.contains("MAC"))
    val MAC_VER = filteredMacDevEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + DVC + entity.platform_name + "." + entity.platform_version + UU, entity.prs_id))
    val MAC_VER_UU = MAC_VER.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    MAC_VER_UU.foreach(println)


    val filCommandEntities = filteredEntities.filter(entity => entity.command != null)

    val RECOVER_U = filCommandEntities.map(entity => (ICDP + "RECOVER" + UU, entity.prs_id))
    val RECOVER_UU = RECOVER_U.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    RECOVER_UU.foreach(println)

    val RECOVER_C = filCommandEntities.map(entity => (ICDP + "RECOVER.CNT", entity.prs_id))
    val RECOVER_CNT = RECOVER_C.aggregateByKey(0)((a, v) => a + 1, _ + _)
    RECOVER_CNT.foreach(println)

    val filtSuccessEntities = filCommandEntities.filter(entity => entity.response != null && entity.response.contains(SUCC_RES_CODE))

    val RECOVER_SUCCESS_U = filtSuccessEntities.map(entity => (ICDP + "RECOVER.SUCCESS" + UU, entity.prs_id))
    val RECOVER_SUCCESS_UU = RECOVER_SUCCESS_U.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    RECOVER_SUCCESS_UU.foreach(println)

    val RECOVER_SUCCESS_C = filtSuccessEntities.map(entity => (ICDP + "RECOVER.SUCCESS.CNT", entity.prs_id))
    val RECOVER_SUCCESS_CNT = RECOVER_SUCCESS_C.aggregateByKey(0)((a, v) => a + 1, _ + _)
    RECOVER_SUCCESS_CNT.foreach(println)


    val filtFailEntities = filCommandEntities.filter(entity => entity.response != null && !entity.response.contains(SUCC_RES_CODE))

    val RECOVER_FAIL_U = filtFailEntities.map(entity => (ICDP + "RECOVER.FAILURE" + UU, entity.prs_id))
    val RECOVER_FAIL_UU = RECOVER_FAIL_U.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    RECOVER_FAIL_UU.foreach(println)

    val RECOVER_FAIL_C = filtFailEntities.map(entity => (ICDP + "RECOVER.FAILURE.CNT", entity.prs_id))
    val RECOVER_FAIL_CNT = RECOVER_FAIL_C.aggregateByKey(0)((a, v) => a + 1, _ + _)
    RECOVER_FAIL_CNT.foreach(println)

    val REC_FAIL_SUCC_UU = (ICDP + "RECOVER.FAILSUCCESS" + UU, RECOVER_SUCCESS_U.map(l => (l._2, "")).join(RECOVER_FAIL_U.map(l => (l._2, ""))).count)
    println(REC_FAIL_SUCC_UU)

    val filtRecordEntities = filCommandEntities.filter(entity => entity.label.contains("record"))

    val RECORD = filtRecordEntities.map(entity => (ICDP + "RECOVER.RECORD.CNT", entity.prs_id))
    val RECORD_CNT = RECORD.aggregateByKey(0)((a, v) => a + 1, _ + _)
    RECORD_CNT.foreach(println)

    val filtRecordSucEnt = filtRecordEntities.filter(entity => entity.response != null && entity.response.contains(SUCC_RES_CODE))

    val RECORD_SUCCESS = filtRecordSucEnt.map(entity => (ICDP + "RECOVER.RECORD.SUCCESS" + UU, entity.prs_id))
    val RECORD_SUCCESS_UU = RECORD_SUCCESS.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    RECORD_SUCCESS_UU.foreach(println)

    val filtRecordFailEnt = filtRecordEntities.filter(entity => entity.response != null && !entity.response.contains(SUCC_RES_CODE))

    val RECORD_FAIL = filtRecordFailEnt.map(entity => (ICDP + "RECOVER.RECORD.FAILURE" + UU, entity.prs_id))
    val RECORD_FAIL_UU = RECORD_FAIL.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    RECORD_FAIL_UU.foreach(println)


    val filtPCFailEntities = filtFailEntities.filter(entity => entity.errorCd.contains("-6015"))

    val PC_FAIL = filtPCFailEntities.map(entity => (ICDP + "RECOVER.PCFAILURE" + UU, entity.prs_id))
    val PC_FAIL_UU = PC_FAIL.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    PC_FAIL_UU.foreach(println)

    val PC_FAIL_C = filtPCFailEntities.map(entity => (ICDP + "RECOVER.PCFAILURE.CNT", entity.prs_id))
    val PC_FAIL_CNT = PC_FAIL_C.aggregateByKey(0)((a, v) => a + 1, _ + _)
    PC_FAIL_CNT.foreach(println)
  }
}
