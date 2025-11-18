import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, regexp_replace}
case class Mesure(Prénom: String,Nom: String)
object DataReader {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF") //Enlève les warnings et une partie des INFO
    val path = "src/Data/station-auber-2021-a-nos-jours.csv"
    val doublon = true

    try{
      import spark.implicits._
      val sep = ";"
      val dataframe = reader(spark, path, doublon, sep) //avec doublon parfait
      dataframe.printSchema()
      val ignore = Seq("DATE/HEURE")
      val datafiltered = dataFrameTransform(dataframe, ignore)
      datafiltered.printSchema()
//      println("Exemple de l'utilisation de la fonction filter")
//      val filtered = dataframe.filter($"Nom" === "Huang")
//      filtered.show()

//      println("Exemple de l'utilisation de la fonction map")
//      val dataset = dataframe.as[Mesure] //"convertis le dataframe en dataset"
//      val nomPersonne = dataset.map(mesure => mesure.Nom.toUpperCase)
//      nomPersonne.show()
    }catch{
      case e: Exception => println(s"Une erreur est survenue: ${e.getMessage}")
    }finally {
      spark.stop()
    }
  }



  def reader(s : SparkSession, path : String, doublon : Boolean, sepa : String) : DataFrame = {
    val res1 = s.read
      .option("header","true")
      .option("delimiter", sepa)
      .option("locale", "fr-FR")  // Indique que la virgule est le séparateur décimal
      .csv(path)
    val res = if (!doublon){
      res1.dropDuplicates()
    }else{
      res1
    }
    res

  }

  def dataFrameTransform(data : DataFrame, ignore : Seq[String]) : DataFrame = {
    val column_to_transform = data.columns.filter(nom => !ignore.contains(nom))
    val inter = data.columns.map{ nomCol =>
      val nomColSecurise = s"`$nomCol`"
      if (column_to_transform.contains(nomCol)){
        regexp_replace(col(nomColSecurise), ",", ".")
          .cast("double")
          .as(nomCol)
      }
      else {
        col(nomCol)
      }
    }
    val res = data.select(inter: _*)
    res
  }
}