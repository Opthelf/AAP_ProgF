import org.apache.spark.sql.SparkSession

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

      reader(spark,path,doublon) //avec doublon parfait

      reader(spark,path,!doublon) //sans doublon parfait
    }catch{
      case e: Exception => println(s"Une erreur est survenue: ${e.getMessage}")
    }finally {
      spark.stop()
    }
  }


  def reader(s : SparkSession, path : String, doublon : Boolean) : Unit = {
    val res1 = s.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(path)
    val res = if (!doublon){
      res1.dropDuplicates()
    }else{
      res1
    }
    res.show()

  }
}