import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Column
case class Mesure(Prénom: String,Nom: String)

object DataReader {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF") //Enlève les warnings et une partie des INFO
    val auber_rer= "src/Data/auber.csv"
    val chatelet_metro = "src/Data/station-chatelet-2021-maintenant.csv"
    val chatelet_rer = "src/Data/station-chatelet-rer-a0.csv"
    val franklin_metro = "src/Data/station-franklin-d-roosevelt-2021-maintenant.csv"
    val nation_rer = "src/Data/station-nation-rer-a0.csv"
    val saint_germain_metro = "src/Data/station-saint-germain-des-pres-de-2024-a-nos-jours-.csv"

    val idf = "src/Data/le-reseau-de-transport-francilien.csv" //À transformer
    val doublon = true

    try{
      //Lecture
      import spark.implicits._
      val sep = ";"
      val ignore = Seq("DATE/HEURE")
      val ignore_minuscule = ignore.map(_.toLowerCase())
      val auberdf = reader(spark, auber_rer, doublon, sep)
      val chatelet_metro_df = reader(spark, chatelet_metro, doublon, sep)
      val chatelet_rer_df = reader(spark, chatelet_rer, doublon, sep)
      val franklin_metro_df = reader(spark, franklin_metro, doublon, sep)
      val nation_rer_df = reader(spark, nation_rer, doublon, sep)
      val saint_germain_metro_df = reader(spark, saint_germain_metro, doublon, sep)
      val idf_df = reader(spark, idf, doublon, sep)

      //Transformation
      val auber_tf = dataFrameTransform(auberdf, ignore)
      val chtlt_m_tf = dataFrameTransform(chatelet_metro_df, ignore)
      val chtlt_r_tf = dataFrameTransform(chatelet_rer_df, ignore_minuscule)
      val fk_tf = dataFrameTransform(franklin_metro_df, ignore_minuscule)
      val nation_tf = dataFrameTransform(nation_rer_df, ignore_minuscule)
      val sg_tf = dataFrameTransform(saint_germain_metro_df,ignore)

      //Transformation Île de France df
      // idf ne possède pas de dates pour ces données on transforme différemment le dataframe
      val idf_tf1 = idf_df.drop("point_geo")
        .drop("mesures_d_amelioration_mises_en_place_ou_prevues") //on a déjà les colonnes long & lat
        .drop("recommandation_de_surveillance")
        .drop("action_s_qai_en_cours")
        .drop("lien_vers_les_mesures_en_direct")
        .drop("air")
        .drop("actions")
        .drop("niveau_pollution") // niveau pollution fait une moyenne de pollution entre p_air & p_particules : p_air & p_particules suffisent
        .drop("niveau_de_pollution")
        .drop("pollution_air")// la donnée est reportée dans niveau sans les incertitudes
        .drop("incertitude") //pour simplifier le problème
        .drop("duree_des_mesures")
      //qualitative -> quantitative
      val idf_tf2 = encodePollutionLevels(idf_tf1)
        .drop("niveau")
        .drop("niveau_de_pollution_aux_particules")
      val idf_tf3 = analyzePollutionRisk(idf_tf2)


      idf_tf3.show()



      //Statistiques
//      auber_tf.describe().show()
//      chtlt_m_tf.describe().show()
//      chtlt_r_tf.describe().show()
//      fk_tf.describe().show()
//      nation_tf.describe().show()
//      sg_tf.describe().show()


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
      .option("inferSchema", "true")
      .csv(path)
    val res = if (!doublon){
      res1.dropDuplicates()
    }else{
      res1
    }
    res

  }
  def dataFrameTransform(data: DataFrame, ignore: Seq[String]): DataFrame = {

    val colsToProcess = data.columns.filterNot(c => ignore.contains(c))

    val transformedCols = data.columns.map { colName =>
      // Sécurisation du nom (backticks)
      val colNameSec = s"`$colName`"

      if (colsToProcess.contains(colName)) {

        // 1. Nettoyage préliminaire :
        // - Trim (enlève les espaces)
        // - Remplace la virgule par un point
        val clean = trim(regexp_replace(col(colNameSec), ",", "."))

        // 2. Logique conditionnelle
        when(clean.contains("<"),
          // CAS "<" (ex: "<2", "<0.5", "< 10")
          // On enlève le symbole "<", on récupère le nombre, et on divise par 2
          regexp_replace(clean, "<", "").cast(DoubleType) / 2
        )
          .when(clean.contains(">"),
            // CAS ">" (ex: ">100", "> 50")
            // On enlève le symbole ">", on récupère le nombre tel quel (plafond bas)
            regexp_replace(clean, ">", "").cast(DoubleType)
          )
          .when(clean.isInCollection(Seq("ND", "n/a", "mq", "-", "vide")),
            // CAS Déchets explicites -> NULL
            lit(null).cast(DoubleType)
          )
          .otherwise(
            // CAS Général : Conversion standard
            // Si Spark n'y arrive pas (ex: texte bizarre), ça deviendra null tout seul
            clean.cast(DoubleType)
          )
          .as(colName)

      } else {
        // Colonnes ignorées (Date, Station...)
        col(colNameSec)
      }
    }

    data.select(transformedCols: _*)
  }

  def encodePollutionLevels(df: DataFrame): DataFrame = {

    // 1. Définir la logique d'encodage
    val encodingLogic = (targetCol: Column) => {
      // On met en majuscules une seule fois pour la robustesse des comparaisons
      val upperCol = upper(targetCol)

      when(upperCol.contains("ÉLEVÉE"), 4) // Cherche 'ÉLEVÉE' dans n'importe quelle longueur de chaîne
        .when(upperCol.contains("MOYENNE"), 3) // Cherche 'MOYENNE' dans la chaîne
        .when(upperCol.contains("FAIBLE"), 2) // Cherche 'FAIBLE'

        // IMPORTANT : On cherche AÉRIENNE ou AERIENNE (sans accent)
        .when(upperCol.contains("AÉRIENNE") || upperCol.contains("AERIENNE"), 1)

        // Gère les cas "PAS DE DONNÉES"
        .when(upperCol.contains("PAS DE DONNÉES") || upperCol.contains("PAS DE DONNEES"), lit(null).cast("int"))

        // Le reste est inconnu
        .otherwise(lit(null).cast("int"))
    }

    // Colonne 1 : niveau_de_pollution_aux_particules (avec backticks pour les espaces)
    val particulesCol = col("`niveau_de_pollution_aux_particules`")

    // Colonne 2 : niveau
    val niveauCol = col("niveau")

    // Appliquer la logique aux deux colonnes
    val df1 = df.withColumn("pollution_particules", encodingLogic(particulesCol))
    val dfFinal = df1.withColumn("pollution_air", encodingLogic(niveauCol))

    dfFinal
  }

  def analyzePollutionRisk(dfEncoded: DataFrame): DataFrame = {
    dfEncoded
      .groupBy(col("pollution_air")) // 1. Créer les groupes de travail
      .agg(count("*").as("nombre_de_stations"))
      // 2. Calculer le risque moyen par station
      .orderBy(col("nombre_de_stations").asc) // 3. Trier le résultat (du pire au meilleur)
  }
}