import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import DataReader.reader
class DataReaderTest extends AnyFunSuite with BeforeAndAfterAll {
  var spark : SparkSession = _ // on n'initialise pas spark pour l'instant

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Test-CSV-Reader")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  //Test 1
  test("La fonction 'reader' doit lire un fichier CSV et compter les lignes"){
    val pathTest = "src/Data/TestProgF.csv"
    val doublon = true
    val read = reader(spark, pathTest, doublon)

    assert(read.count() == 4)
    assert(read.columns.contains("Nom"))
    assert(!read.columns.contains("Prix"))



  }
  //Test 2
  test("la fonction 'reader' doit pouvoir supprimer les doublons parfaits"){
    val pathTest = "src/Data/TestProgF.csv"
    val doublon2 = false
    val read = reader(spark, pathTest, doublon2)
    assert(read.count() == 3)

  }


}
