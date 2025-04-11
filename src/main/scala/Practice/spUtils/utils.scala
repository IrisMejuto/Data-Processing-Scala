package Practice.spUtils

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File

object utils {
    /**
     * Leer csv
     * Excepción si no encuentra el archivo
     *
     * @param path Ruta relativa
     * @param spark Sesión de spark implícita
     * @return DataFrame cargado desde csv
     */
    def readCsv(path: String)(implicit spark: SparkSession): DataFrame = {
      // Obtener el directorio
      val currentDir = System.getProperty("user.dir")

      // Construir la ruta
      val filePath = s"$currentDir/src/test/resources/$path"

      // Verificar si el archivo existe
      val file = new File(filePath)
      if (!file.exists()) {
        throw new IllegalArgumentException(s"Archivo no encontrado: $path")
      }

      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(filePath)
    }

}
