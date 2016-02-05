import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression

object TestBasketBallPlayer {

  def main(args: Array[String]) {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Mlib-Demo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val player1 = new LabeledPoint(1.0, Vectors.dense(Array(80.0, 180)))
    val player2 = new LabeledPoint(1.0, Vectors.dense(Array(85.0, 210)))
    val player3 = new LabeledPoint(1.0, Vectors.dense(Array(90.0, 200)))

    val nonPlayer1 = new LabeledPoint(0.0, Vectors.dense(Array(60.0, 170)))
    val nonPlayer2 = new LabeledPoint(0.0, Vectors.dense(Array(65.0, 180)))
    val nonPlayer3 = new LabeledPoint(0.0, Vectors.dense(Array(70.0, 172)))

    val trainingRDD = sc.parallelize(List(player1, player2, player3, nonPlayer1, nonPlayer2, nonPlayer3))

    val trainingDF = trainingRDD.toDF

    val estimator = new LogisticRegression

    val transformer = estimator.fit(trainingDF)

    val testRDD = sc.parallelize(List(Vectors.dense(72.0, 176), Vectors.dense(61.0, 168.0)))

    case class Feature(v: Vector)

    //This line not working
    val featuresRDD = testRDD.map(_).toDF("features")
  }
}
