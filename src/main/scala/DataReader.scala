package projet
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, min, max, lit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
object DataReader {
  val auber_path= "src/Data/auber.csv"
  val chatelet_rer_path = "src/Data/station-chatelet-rer-a0.csv"
  val franklin_metro_path = "src/Data/station-franklin-d-roosevelt-2021-maintenant.csv"
  val nation_rer_path = "src/Data/station-nation-rer-a0.csv"
  val saint_germain_metro_path = "src/Data/station-saint-germain-des-pres-de-2024-a-nos-jours-.csv"
  val idf_path = "src/Data/idf_nettoye/part-00000-7f64d0ed-e28c-494e-9bf4-13ed266ae0a0-c000.csv"

  lazy val spark: SparkSession = {
    // 1. Création dans une variable temporaire
    val session = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]") // [*] est mieux : utilise tous les coeurs du CPU
      // On garde les configs de sécurité au cas où (ne fait pas de mal)
      .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
      .getOrCreate()

    // 2. Configuration sur la variable temporaire
    session.sparkContext.setLogLevel("OFF")

    // 3. IMPORTANT : On retourne l'objet à la fin pour qu'il soit affecté à 'spark'
    session
  }
  val sep = ";"
  val ignore = Seq("DATE/HEURE")
  val ignore_minuscule = ignore.map(_.toLowerCase())
  val doublon = true
  val index = "Indicateur_Pollution_Global"

  lazy val auberdf : DataFrame = {
    println("Chargement d'Auber RER")
    // Étape 1 : Lecture
    val dfRaw = reader(spark, auber_path, doublon, sep)

    // Étape 2 : Transformation
    val dfTf = dataFrameTransform(dfRaw, ignore)

    // Étape 3 : Nettoyage (Outliers)
    val dfClean = remplacerOutliersParNull(dfTf, Array("NO", "NO2", "PM10", "PM2_5", "CO2", "TEMP", "HUMI"))

    // Étape 4 : Calcul Indicateur
    val dfIndic = calculerIndicateurDynamique(dfClean)

    // Étape 5 : Analyse (Pic journalier) - C'est le résultat final retourné
    analyzeDailyPeak(dfIndic, index)

  }
  lazy val chateletdf : DataFrame = {
    println("Chargement de Chatelet RER")
    // Étape 1 : Lecture
    val dfRaw = reader(spark, chatelet_rer_path, doublon, sep)

    // Étape 2 : Transformation
    val dfTf = dataFrameTransform(dfRaw, ignore_minuscule)

    // Étape 3 : Nettoyage (Outliers)
    val dfClean = remplacerOutliersParNull(dfTf, Array("PM10","TEMP","HUMI"))

    // Étape 4 : Calcul Indicateur
    val dfIndic = calculerIndicateurDynamique(dfClean)

    // Étape 5 : Analyse (Pic journalier) - C'est le résultat final retourné
    analyzeDailyPeak(dfIndic, index)

  }
  lazy val nationdf : DataFrame = {
    println("Chargement de Nation RER")
    // Étape 1 : Lecture
    val dfRaw = reader(spark, nation_rer_path, doublon, sep)

    // Étape 2 : Transformation
    val dfTf = dataFrameTransform(dfRaw, ignore_minuscule)

    // Étape 3 : Nettoyage (Outliers)
    val dfClean = remplacerOutliersParNull(dfTf, Array("PM10","PM2_5","TEMP","HUMI"))

    // Étape 4 : Calcul Indicateur
    val dfIndic = calculerIndicateurDynamique(dfClean)

    // Étape 5 : Analyse (Pic journalier) - C'est le résultat final retourné
    analyzeDailyPeak(dfIndic, index)

  }
  lazy val saintgermaindf : DataFrame = {
    println("Chargement de Saint Germain des Près Métro")
    // Étape 1 : Lecture
    val dfRaw = reader(spark, saint_germain_metro_path, doublon, sep)

    // Étape 2 : Transformation
    val dfTf = dataFrameTransform(dfRaw, ignore)

    // Étape 3 : Nettoyage (Outliers)
    val dfClean = remplacerOutliersParNull(dfTf, Array("PM10","TEMP","HUMI"))

    // Étape 4 : Calcul Indicateur
    val dfIndic = calculerIndicateurDynamique(dfClean)

    // Étape 5 : Analyse (Pic journalier) - C'est le résultat final retourné
    analyzeDailyPeak(dfIndic, index)

  }
  lazy val franklindf : DataFrame = {
    println("Chargement de Franklin-D-Roosevelt Métro")
    // Étape 1 : Lecture
    val dfRaw = reader(spark, franklin_metro_path, doublon, sep)

    // Étape 2 : Transformation
    val dfTf = dataFrameTransform(dfRaw, ignore_minuscule)

    // Étape 3 : Nettoyage (Outliers)
    val dfClean = remplacerOutliersParNull(dfTf, Array("NO","NO2","PM10","CO2","TEMP","HUMI"))

    // Étape 4 : Calcul Indicateur
    val dfIndic = calculerIndicateurDynamique(dfClean)

    // Étape 5 : Analyse (Pic journalier) - C'est le résultat final retourné
    analyzeDailyPeak(dfIndic, index)

  }
  lazy val reseauidf_df : DataFrame = {
    println("Chargement de Franklin-D-Roosevelt Métro")
    // Étape 1 : Lecture
    val dfRaw = reader(spark,idf_path, doublon, sep)
    dfRaw

  }





  def main(args: Array[String]): Unit = {


    val auber_rer= "src/Data/auber.csv"
    val chatelet_metro = "src/Data/station-chatelet-2021-maintenant.csv"
    val chatelet_rer = "src/Data/station-chatelet-rer-a0.csv"
    val franklin_metro = "src/Data/station-franklin-d-roosevelt-2021-maintenant.csv"
    val nation_rer = "src/Data/station-nation-rer-a0.csv"
    val saint_germain_metro = "src/Data/station-saint-germain-des-pres-de-2024-a-nos-jours-.csv"

     //À transformer

    try{
      //Lecture
      import spark.implicits._
      val sep = ";"
      val ignore = Seq("DATE/HEURE")
      val ignore_minuscule = ignore.map(_.toLowerCase())

      val chatelet_metro_df = reader(spark, chatelet_metro, doublon, sep)
      val chatelet_rer_df = reader(spark, chatelet_rer, doublon, sep)
      val franklin_metro_df = reader(spark, franklin_metro, doublon, sep)
      val nation_rer_df = reader(spark, nation_rer, doublon, sep)
      val saint_germain_metro_df = reader(spark, saint_germain_metro, doublon, sep)
      val idf_df = reader(spark, idf_path, doublon, sep)
      val idf_final = analyzePollution(idf_df)
      idf_final.show()

      //Transformation

      //Transformation Île de France df
//      val idf_tf1 = idf_df.drop("point_geo")
//        .drop("mesures_d_amelioration_mises_en_place_ou_prevues") //on a déjà les colonnes long & lat
//        .drop("recommandation_de_surveillance")
//        .drop("action_s_qai_en_cours")
//        .drop("lien_vers_les_mesures_en_direct")
//        .drop("air")
//        .drop("actions")
//        .drop("niveau_pollution") // niveau pollution fait une moyenne de pollution entre p_air & p_particules : p_air & p_particules suffisent
//        .drop("niveau_de_pollution")
//        .drop("pollution_air")// la donnée est reportée dans niveau sans les incertitudes
//        .drop("incertitude") //pour simplifier le problème
//        .drop("duree_des_mesures")

//
////      //qualitative -> quantitative
//      val idf_tf2 = encodePollutionLevels(idf_tf1)
//        .drop("niveau")
//        .drop("niveau_de_pollution_aux_particules")
//        .drop("pollution_particules")
//        .drop("identifiant_station")
//
//      idf_tf2
//        .coalesce(1)
//        .write
//        .option("header","true")
//        .option("delimiter",";")
//        .mode("overwrite")
//        .csv("src/Data/idf_nettoye")

      //Statistiques



//      val idf_anaylze = analyzePollution(idf_df)
//      val idf_particles_analyze = analyzeParticlesPollution(idf_tf2)
//      idf_anaylze.orderBy("Ligne").show(25)
//      val auber_clean = remplacerOutliersParNull(auber_tf,Array("NO", "NO2", "PM10", "PM2_5", "CO2", "TEMP", "HUMI"))
//      val chatelet_clean = remplacerOutliersParNull(chatelet_r_tf,Array("PM10","TEMP","HUMI"))
//      val nation_clean = remplacerOutliersParNull(nation_tf,Array("PM10","PM2_5","TEMP","HUMI"))
//      val franklin_clean = remplacerOutliersParNull(fk_tf,Array("NO","NO2","PM10","CO2","TEMP","HUMI"))
//      val saint_germain_clean = remplacerOutliersParNull(sg_tf,Array("PM10","TEMP","HUMI"))
//









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

      when(upperCol.contains("ÉLEVÉE"), 1) // Cherche 'ÉLEVÉE' dans n'importe quelle longueur de chaîne
        .when(upperCol.contains("MOYENNE"), 0.75) // Cherche 'MOYENNE' dans la chaîne
        .when(upperCol.contains("FAIBLE"), 0.5) // Cherche 'FAIBLE'

        // IMPORTANT : On cherche AÉRIENNE ou AERIENNE (sans accent)
        .when(upperCol.contains("AÉRIENNE") || upperCol.contains("AERIENNE"), 0.25)

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

    //println("--- Nettoyage en cours (Remplacement par NULL) ---")

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

    dfResultat
  }

  def calculerIndicateurDynamique(df: DataFrame): DataFrame = {

    // 1. Définition de la configuration idéale (Si tout est présent)
    // Map("NomColonne" -> (Seuil/Ref, Poids_Ideal))
    val configPolluants = Map(
      "NO2"   -> (200.0, 0.20),
      "PM10"  -> (50.0,  0.15),
      "PM2_5" -> (25.0,  0.35),
      "NO"    -> (400.0, 0.10)
    )

    // Configuration spécifique pour CO2 et Météo (Ref, Poids)
    // Note: On les sépare car la formule de calcul du score est différente
    val configCO2 = Map("CO2" -> (1200.0, 0.10))
    val configMeteo = Map(
      "TEMP" -> (15.0, 0.05), // Ici 15.0 est l'écart max
      "HUMI" -> (40.0, 0.05)  // Ici 40.0 est l'écart max
    )

    // 2. Vérification des colonnes présentes dans le DataFrame
    val colonnesDispo = df.columns.toSet

    // 3. Calcul de la Somme des Poids des colonnes présentes
    // On ne garde que les poids des colonnes qui existent vraiment dans 'df'
    var sommePoids = 0.0

    (configPolluants ++ configCO2 ++ configMeteo).foreach { case (colName, (_, poids)) =>
      if (colonnesDispo.contains(colName)) {
        sommePoids += poids
      }
    }

    // Sécurité : Si aucune colonne n'est trouvée (fichier vide ou erreurs noms), on renvoie le DF tel quel
    if (sommePoids == 0.0) return df.withColumn("Indicateur_Synthetique", lit(0.0))

//    println(s"Colonnes détectées. Somme des poids bruts : $sommePoids. Recalcul des poids en cours...")

    // 4. Construction de la liste des expressions pondérées
    var expressionsScores: List[Column] = List()

    // --- A. Traitement des Polluants Classiques (Simple division) ---
    configPolluants.foreach { case (colName, (ref, poidsOriginal)) =>
      if (colonnesDispo.contains(colName)) {
        // Nouveau poids ajusté
        val poidsAjuste = poidsOriginal / sommePoids

        // Score normalisé (Valeur / Ref)
        val score = coalesce(col(colName), lit(0)) / ref

        // Ajout à la liste : Score * Poids
        expressionsScores = expressionsScores :+ (score * poidsAjuste)
      }
    }

    // --- B. Traitement du CO2 (Formule avec soustraction du bruit de fond) ---
    if (colonnesDispo.contains("CO2")) {
      val (ref, poidsOriginal) = configCO2("CO2")
      val poidsAjuste = poidsOriginal / sommePoids

      // (Val - 400) / (Ref - 400)
      val co2Net = when((coalesce(col("CO2"), lit(400)) - 400) < 0, 0)
        .otherwise(coalesce(col("CO2"), lit(400)) - 400)
      val score = co2Net / (ref - 400)

      expressionsScores = expressionsScores :+ (score * poidsAjuste)
    }

    // --- C. Traitement Météo (Distance à l'idéal) ---
    configMeteo.foreach { case (colName, (ecartMax, poidsOriginal)) =>
      if (colonnesDispo.contains(colName)) {
        val poidsAjuste = poidsOriginal / sommePoids
        val ideal = if (colName == "TEMP") 20.0 else 50.0

        val score = abs(coalesce(col(colName), lit(ideal)) - ideal) / ecartMax
        expressionsScores = expressionsScores :+ (score * poidsAjuste)
      }
    }

    // 5. Somme finale de toutes les expressions
    val indicateurFinal = expressionsScores.reduce(_ + _)

    df.withColumn("Indicateur_Pollution_Global", indicateurFinal)
  }




}

