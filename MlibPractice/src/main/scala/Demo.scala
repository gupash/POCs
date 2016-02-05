import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry, CoordinateMatrix}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]) {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Mlib-Demo").setMaster("local")
    val sc = new SparkContext(conf)

    val persons = MLUtils.loadLibSVMFile(sc, getClass.getClassLoader.getResource("person_libsvm.txt").getPath)

    val people = Matrices.dense(3, 2, Array(170.0, 65.0, 27.0, 160d, 60d, 23d))

    val peoplesRDD = sc.parallelize(List(persons.first.features, persons.take(2).last.features))

    val persRowMatrix = new RowMatrix(peoplesRDD)

    //println("Rows : " + persRowMatrix.numRows+ "\t" + "Columns : " + persRowMatrix.numCols)

    val personRDD = sc.parallelize(List(IndexedRow(0L, persons.first.features), IndexedRow(1L, persons.take(2).last.features)))

    val indexedRowMatrix = new IndexedRowMatrix(personRDD)

    val meRDD = sc.parallelize(List(MatrixEntry(0, 0, 150), MatrixEntry(0, 1, 60), MatrixEntry(0, 2, 25),
      MatrixEntry(1, 0, 170), MatrixEntry(1, 1, 62), MatrixEntry(1, 2, 27)))

    val coordMatrix = new CoordinateMatrix(meRDD)

    val summary = Statistics.colStats(peoplesRDD)

    peoplesRDD.collect.foreach(println)

    println("Count : " + summary.count + "\t" + "Max : " + summary.max + "\t" + "Mean : " + "\t" + summary.mean + "\t" + "Variance : " + summary.variance)
  }
}
