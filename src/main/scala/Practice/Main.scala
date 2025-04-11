package Practice
import org.apache.spark.sql.SparkSession
import Practice.spUtils.sparkUtils

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = sparkUtils.getSparkSession("Examen")
    println("Hello world!")

    spark.stop()
  }
}