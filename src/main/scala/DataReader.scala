import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, min, max, lit}

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



      //Statistiques



      //val idf_anaylze = analyzePollution(idf_tf2)
//      val idf_particles_analyze = analyzeParticlesPollution(idf_tf2)
      //idf_anaylze.show()
      val auber_clean = remplacerOutliersParNull(auber_tf,Array("NO", "NO2", "PM10", "PM2_5", "CO2", "TEMP", "HUMI"))


      //Période critique & Pics horaires
      // Liste des colonnes à analyser (on exclut le timestamp)
      // Utilisation
      val auber_IG = calculerIndicateurPondere(auber_clean)
      val df_final = analyzeDailyPeak(auber_IG,"Indicateur_Pollution_Global")
      val dfDebug = auber_IG.withColumn("heure", hour(col("DATE/HEURE")))

      // On regarde la moyenne de CHAQUE sous-indice par heure
      dfDebug.groupBy("heure")
        .agg(
          avg("Indicateur_Pollution_Global").as("Global"),
          avg("NO2").as("Score_NO2"),     // Est-ce le diesel des travaux ?
          avg("PM10").as("Score_PM10"),   // Est-ce la poussière des travaux ?
          avg("CO2").as("Score_CO2"),     // Est-ce l'arrêt de la ventilation ?
          count("*").as("Nb_Mesures")         // Y a-t-il très peu de données à 3h ?
        )
        .orderBy("heure")
        .show(24)



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

  def analyzePollution(dfEncoded: DataFrame): DataFrame = { //Par ligne
    val dfStationsParNiveau = dfEncoded
      // 1. Regrouper par le niveau de pollution
      .groupBy(col("nom_de_la_ligne").as("Ligne"))
      .agg(
        // 2. Compter le nombre de stations dans ce groupe
        avg(col("pollution_air")).as("moyenne_pollution_air"),
        avg(col("pollution_particules")).as("moyenne_pollution_particules"),
        count("*").as("total_stations_par_ligne")
      )
    dfStationsParNiveau
  }

  def analyzeDailyPeak(df : DataFrame, index : String): DataFrame = {
    val res = df.withColumn("année", year(col("DATE/HEURE")))
      .withColumn("heure", hour(col("DATE/HEURE")))
      .drop(col("DATE/HEURE"))

      .groupBy("heure")
      .agg(avg(col(index)))
      .orderBy("heure")

    res
  }

  def remplacerOutliersParNull(df: DataFrame, colonnes: Array[String]): DataFrame = {

    var dfResultat = df
    val boundsMap = scala.collection.mutable.Map[String, (Double, Double)]()

    println("--- Nettoyage en cours (Remplacement par NULL) ---")

    colonnes.foreach { colName =>
      // 1. Calculs Statistiques (IQR)
      val quantiles = dfResultat.stat.approxQuantile(colName, Array(0.25, 0.75), 0.01)
      val q1 = quantiles(0)
      val q3 = quantiles(1)
      val iqr = q3 - q1

      val rawLowerBound = q1 - 1.5 * iqr
      val upperBound = q3 + 1.5 * iqr

      // 2. Correction Physique (Empêcher le négatif sauf pour TEMP)
      val lowerBound = if (colName == "TEMP") {
        rawLowerBound
      } else {
        math.max(0.0, rawLowerBound)
      }

      // Stockage pour le rapport
      boundsMap += (colName -> (lowerBound, upperBound))

      // 3. Remplacement conditionnel
      // SI (valeur < min OU valeur > max) ALORS null SINON garder valeur
      dfResultat = dfResultat.withColumn(colName,
        when(col(colName) < lowerBound || col(colName) > upperBound, lit(null))
          .otherwise(col(colName))
      )
    }

    // 4. Rapport
    println("--- Bornes appliquées ---")
    boundsMap.foreach { case (k, v) =>
      println(f"$k%s : [${v._1}%.2f, ${v._2}%.2f]")
    }

    return dfResultat
  }


  def calculerIndicateurPondere(df: DataFrame): DataFrame = {

    // --- 1. CONFIGURATION DES SEUILS (Référentiels "Mauvais") ---
    // Valeurs utilisées pour diviser la mesure. Si Valeur = Seuil, Score = 1.0.
    val refNO2 = 200.0    // Seuil horaire OMS
    val refPM10 = 50.0    // Seuil journalier UE
    val refPM2_5 = 25.0   // Seuil OMS (le plus strict)
    val refNO = 400.0     // Indicatif (tolérance haute)
    val refCO2 = 1200.0   // Seuil de confinement élevé (air vicié)

    // Pour la météo : Ecart maximal toléré par rapport à l'idéal avant d'avoir un score de 1.0
    val maxEcartTemp = 15.0 // Si on s'éloigne de 15°C de l'idéal (donc <5°C ou >35°C), score = 1
    val maxEcartHumi = 40.0 // Si on s'éloigne de 40% (donc <10% ou >90%), score = 1

    // --- 2. CONFIGURATION DES POIDS (Importance relative) ---
    // Total = 1.0
    val w_PM2_5 = 0.35 // Très toxique
    val w_NO2   = 0.20 // Toxique
    val w_PM10  = 0.15 // Particules
    val w_NO    = 0.10 // Precurseur
    val w_CO2   = 0.10 // Confinement
    val w_TEMP  = 0.05 // Inconfort
    val w_HUMI  = 0.05 // Inconfort

    // --- 3. NORMALISATION (Score de 0 à 1 par colonne) ---

    // A. Polluants : Valeur / Référence
    // On utilise coalesce(..., 0) pour gérer les nulls (si capteur HS, on compte 0 pollution pour ce capteur)
    val scNO2 = coalesce(col("NO2"), lit(0)) / refNO2
    val scPM10 = coalesce(col("PM10"), lit(0)) / refPM10
    val scPM2_5 = coalesce(col("PM2_5"), lit(0)) / refPM2_5
    val scNO = coalesce(col("NO"), lit(0)) / refNO

    // B. CO2 : On enlève le bruit de fond extérieur (~400 ppm) pour ne noter que l'ajout
    // Formule : (CO2 - 400) / (1200 - 400). Borné à 0 min.
    val co2Net = when((coalesce(col("CO2"), lit(400)) - 400) < 0, 0).otherwise(coalesce(col("CO2"), lit(400)) - 400)
    val scCO2 = co2Net / (refCO2 - 400)

    // C. Météo : Distance de l'idéal (20°C et 50%)
    // Score = |Valeur - Idéal| / EcartMax
    val distTemp = abs(coalesce(col("TEMP"), lit(20)) - 20) / maxEcartTemp
    val distHumi = abs(coalesce(col("HUMI"), lit(50)) - 50) / maxEcartHumi

    // --- 4. CALCUL DE L'INDICE PONDÉRÉ ---
    val indicateur = (
      (scPM2_5 * w_PM2_5) +
        (scNO2   * w_NO2) +
        (scPM10  * w_PM10) +
        (scNO    * w_NO) +
        (scCO2   * w_CO2) +
        (distTemp * w_TEMP) +
        (distHumi * w_HUMI)
      )

    // --- 5. RESULTAT ---
    df.withColumn("Indicateur_Pollution_Global", indicateur)
  }


}

