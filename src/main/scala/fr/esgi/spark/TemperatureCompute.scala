package fr.esgi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.sql.Date

object TemperatureCompute {

  case class Temperature(jour : Date, departement  : Int, population   : Long, temperature : Int, id_station: String)
  case class Communes(DEPCOM : String, PTOT : String)
  case class Geoflar(Insee : String, Code_Dept: String, grom : String)
  case class PostesSynop(ID : String, Latitude : String, Longitude : String)
  case class Synop(t : Long, numer_sta : String, Date : String)


  def main(args: Array[String]){

    val spark = SparkSession.builder().appName("temperature").config("spark.sql.autoBroadcastJoinThreshold", "-1").master("local[*]").getOrCreate()
    import spark.implicits._

    val communes_df = spark
                        .read.option("header", false)
                        .option("delimiter", ";")
                        .csv("src/main/resources/Communes.csv")
                        .map(row => Communes(row.getString(0), row.getString(4)))

    val code_insee_postaux_geoflar_df = spark
                  .read
                  .option("header", false)
                  .option("delimiter", ";")
                  .csv("src/main/resources/code-insee-postaux-geoflar.csv")
                  .map(row => Geoflar(row.getString(0), row.getString(11), row.getString(13)))

    val poste_synop_df = spark
                          .read.option("header", false)
                          .option("delimiter", ";")
                          .csv("src/main/resources/postesSynop.txt")
                          .map(row => PostesSynop(row.getString(0), row.getString(2), row.getString(3)))

    val synop_df = spark
                    .read.option("header", false)
                    .option("delimiter", ";")
                    .csv("src/main/resources/synop.2020120512.txt")
                    .map(row => Synop(row.getLong(6), row.getString(0), row.getString(1)))

  val udf_geo = udf((s1 : String, s2 : String) => s1+","+s2)
    val udf_temperature = udf(t : Long=> t - 273.15)
  val temperature_date_and_geo = synop_df.join(poste_synop_df, synop_df("numer_sta") === poste_synop_df("ID"), "left")
  val temperature_concate_geo = temperature_date_and_geo.withColumn("geo", udf_geo(temperature_date_and_geo("Latitude"),temperature_date_and_geo("Longitude")))
  val temperature_with_code_dept = temperature_concate_geo.join(code_insee_postaux_geoflar_df, temperature_concate_geo("geo") === code_insee_postaux_geoflar_df("grom"), "left")
    val temperature_with_population = temperature_with_code_dept.join(communes_df, temperature_with_code_dept("Insee") === communes_df("DEPCOM"), "left")

  val temperature_with_populaire = temperature_with_population.map(row => Temperature(row(2), row(8), row(11), row(0), row(1)))
  val temperature_transform = temperature_with_populaire.withColumn("temperature", udf_temperature(col(t)))

  }
}
