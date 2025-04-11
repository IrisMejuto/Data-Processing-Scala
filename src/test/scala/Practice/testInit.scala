package utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import java.io.File
import org.apache.commons.io.FileUtils
import scala.reflect.io.Directory

trait SparkSessionTestWrapper {
  // Eliminar metastore_db y tmp
  FileUtils.deleteDirectory(new File("metastore_db"))
  new Directory(new File("src/test/resources/tmp")).deleteRecursively()

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("spark-test")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.codegen.wholeStage", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("hive.exec.dynamic.partition.process_type", "nonstrict")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
}

case class testInit() extends FlatSpec with Matchers with BeforeAndAfterAll with SparkSessionTestWrapper {

  // Crea un dataframe desde una lista de Rows y un schema
  def newDf(datos: Seq[Row], schema: StructType): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(datos), schema)

  // Compara dos DataFrames
  def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean = true): DataFrame = {
    val newSchema = StructType(df.schema.map {
      case StructField(name, dataType, _, metadata) => StructField(name, dataType, nullable, metadata)
    })
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  def checkDf(expected: DataFrame, actual: DataFrame): Unit = {
    expected.schema.toString shouldBe actual.schema.toString
    expected.collectAsList() shouldBe actual.collectAsList()
  }

  def checkDfIgnoreDefault(expected: DataFrame, actual: DataFrame): Unit = {
    setNullableStateForAllColumns(expected).schema.toString shouldBe setNullableStateForAllColumns(actual).schema.toString
    expected.collectAsList() shouldBe actual.collectAsList()
  }

}
