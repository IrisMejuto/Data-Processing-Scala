package Practice

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   * - Muestra el esquema del DataFrame. (hazlo en el test)
   * - Filtra los estudiantes con una calificación mayor a 8.
   * - Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    estudiantes
      .filter(col("calificacion") > 8)
      .select("nombre", "calificacion")
      .orderBy(desc("calificacion"))
  }

  /** Ejercicio 2: UDF (User Defined Function)
   * Pregunta: Define una función que determine si un número es par o impar.
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val esParUdf = udf((num: Int) => if (num % 2 == 0) "Par" else "Impar")
    numeros.withColumn("par", esParUdf(col("cantidad")))
  }

  /** Ejercicio 3: Joins y agregaciones
   * Pregunta: Dado dos DataFrames,
   * uno con información de estudiantes (id, nombre)
   * y otro con calificaciones (id_estudiante, asignatura, calificacion),
   * realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .groupBy("nombre")
      .agg(round(avg("calificacion"), 2).alias("media"))
  }

  /** Ejercicio 4: Uso de RDDs
   * Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   */
  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(palabras)
    rdd.map(palabra => (palabra, 1)).reduceByKey(_ + _)
  }

  /** Ejercicio 5: Procesamiento de archivos
   * Pregunta: Carga un archivo CSV que contenga información sobre
   * ventas (id_venta, id_producto, cantidad, precio_unitario)
   * y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */
  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {
    ventas
      .withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso_total").alias("ingreso_total_producto"))
      .orderBy("id_producto")
  }

}
