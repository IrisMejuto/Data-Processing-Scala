package Practice.spUtils

import org.apache.spark.sql.SparkSession

object sparkUtils {

  def getSparkSession(appName: String = "PracticeApp"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }

}