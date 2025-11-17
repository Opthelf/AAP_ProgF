import org.apache.spark.sql.{DataFrame, SparkSession}

case class Mesure(Prénom: String,Nom: String)
object DataReader {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF") //Enlève les warnings et une partie des INFO
    val path = "src/Data/TestProgF.csv"
    val doublon = true

    try{
      import spark.implicits._
      val dataframe = reader(spark,path,doublon) //avec doublon parfait

      println("Exemple de l'utilisation de la fonction filter")
      val filtered = dataframe.filter($"Nom" === "Huang")
      filtered.show()

      println("Exemple de l'utilisation de la fonction map")
      val dataset = dataframe.as[Mesure] //"convertis le dataframe en dataset"
      val nomPersonne = dataset.map(mesure => mesure.Nom.toUpperCase)
      nomPersonne.show()
    }catch{
      case e: Exception => println(s"Une erreur est survenue: ${e.getMessage}")
    }finally {
      spark.stop()
    }
  }


  def reader(s : SparkSession, path : String, doublon : Boolean) : DataFrame = {
    val res1 = s.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(path)
    val res = if (!doublon){
      res1.dropDuplicates()
    }else{
      res1
    }
    res

  }
}