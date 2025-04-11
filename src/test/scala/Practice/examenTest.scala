package Practice

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import Practice.examen._
import Practice.spUtils.utils.readCsv
import utils.testInit

class examenTest extends testInit {

  behavior of "examen"

  it should "resolver ejercicio1: filtrar y ordenar estudiantes" in {
    val schema = StructType(Seq(
      StructField("nombre", StringType, false),
      StructField("edad", IntegerType, false),
      StructField("calificacion", DoubleType, false)
    ))

    val datos = Seq(
      Row("Pepe", 20, 9.5),
      Row("Eva", 22, 7.6),
      Row("María", 21, 8.9),
      Row("Juan", 23, 6.5),
      Row("Laura", 19, 9.2)
    )

    val df = newDf(datos, schema)
    df.printSchema()
    val res = ejercicio1(df)
    res.show()
  }

  it should "resolver ejercicio2: par o impar" in {
    val schema = StructType(Seq(StructField("cantidad", IntegerType, false)))
    val datos = Seq(Row(1), Row(2), Row(3), Row(4))
    val df = newDf(datos, schema)

    val res = ejercicio2(df)
    res.show()
  }

  it should "resolver ejercicio3: join y media de calificaciones" in {
    val schemaEstudiantes = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("nombre", StringType, false)
    ))

    val schemaNotas = StructType(Seq(
      StructField("id_estudiante", IntegerType, false),
      StructField("asignatura", StringType, false),
      StructField("calificacion", DoubleType, false)
    ))

    val estudiantes = Seq(
      Row(1, "Juan"),
      Row(2, "Ana"),
      Row(3, "Inés")
    )

    val notas = Seq(
      Row(1, "Visualización", 7.5),
      Row(1, "Data Arquitecture", 8.0),
      Row(2, "Data Processing", 9.0),
      Row(2, "Machine Learning", 9.5),
      Row(3, "SQL", 7.5)
    )

    val dfEstudiantes = newDf(estudiantes, schemaEstudiantes)
    val dfNotas = newDf(notas, schemaNotas)

    val res = ejercicio3(dfEstudiantes, dfNotas)
    res.show()
  }

  it should "resolver ejercicio4: contar ocurrencias en RDD" in {
    val palabras = List("lápiz", "coche", "maleta", "impresora", "perro", "uva")
    val res: RDD[(String, Int)] = ejercicio4(palabras)
    res.collect().foreach(println)
  }

  it should "resolver ejercicio5: calcular ingreso total por producto" in {
    val dfVentas = readCsv("ventas.csv")
    val res: DataFrame = ejercicio5(dfVentas)
    res.show()
  }


}
