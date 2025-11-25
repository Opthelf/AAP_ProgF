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

    val idf = "src/Data/idf_nettoye/part-00000-48cf0098-677d-4a0d-8f77-74207e9c408e-c000.csv" //À transformer
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
      val chatelet_m_tf = dataFrameTransform(chatelet_metro_df, ignore)
      val chatelet_r_tf = dataFrameTransform(chatelet_rer_df, ignore_minuscule)
      val fk_tf = dataFrameTransform(franklin_metro_df, ignore_minuscule)
      val nation_tf = dataFrameTransform(nation_rer_df, ignore_minuscule)
      val sg_tf = dataFrameTransform(saint_germain_metro_df,ignore)

      //Transformation Île de France df
      // idf ne possède pas de dates pour ces données on transforme différemment le dataframe
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
//      //qualitative -> quantitative
//      val idf_tf2 = encodePollutionLevels(idf_tf1)
//        .drop("niveau")
//        .drop("niveau_de_pollution_aux_particules")

//      idf_tf2
//        .coalesce(1)
//        .write
//        .option("header","true")
//        .option("delimiter",";")
//        .mode("overwrite")
//        .csv("src/Data/idf_nettoye")

      //Statistiques



      val idf_anaylze = analyzePollution(idf_df)
//      val idf_particles_analyze = analyzeParticlesPollution(idf_tf2)
      //idf_anaylze.orderBy("Ligne").show(25)
      val auber_clean = remplacerOutliersParNull(auber_tf,Array("NO", "NO2", "PM10", "PM2_5", "CO2", "TEMP", "HUMI"))
      val chatelet_clean = remplacerOutliersParNull(chatelet_r_tf,Array("PM10","TEMP","HUMI"))
      val nation_clean = remplacerOutliersParNull(nation_tf,Array("PM10","PM2_5","TEMP","HUMI"))
      val franklin_clean = remplacerOutliersParNull(fk_tf,Array("NO","NO2","PM10","CO2","TEMP","HUMI"))
      val saint_germain_clean = remplacerOutliersParNull(sg_tf,Array("PM10","TEMP","HUMI"))



      //Période critique & Pics horaires
      // Utilisation
      val auber_IG = calculerIndicateurDynamique(auber_clean)
      val chatelet_IG = calculerIndicateurDynamique(chatelet_clean)
      val nation_IG = calculerIndicateurDynamique(nation_clean)
      val franklin_IG = calculerIndicateurDynamique(franklin_clean)
      val saint_germain_IG = calculerIndicateurDynamique(saint_germain_clean)

      val index = "Indicateur_Pollution_Global"
      val auber_final = analyzeDailyPeak(auber_IG, index)
      val chatelet_final = analyzeDailyPeak(chatelet_IG, index)
      val nation_final = analyzeDailyPeak(chatelet_IG, index)
      val franklin_final = analyzeDailyPeak(franklin_IG, index)
      val saint_germain_final = analyzeDailyPeak(saint_germain_IG, index)

//      println("Analyse auber rer")
      //auber_final.show(24)
//      println("Analyse chatelet rer")
//      chatelet_final.show(24)
//      println("Analyse nation rer")
//      nation_final.show(24)
//      println("Analyse franklin metro")
//      franklin_final.show(24)
//      println("Analyse saint_germain metro")
//      saint_germain_final.show(24)

      // 1. On transforme le tableau de 24h en une seule ligne avec le nom
      val dfAuberReady = auber_final
        .agg(avg("avg(Indicateur_Pollution_Global)").as("Indicateur_Synthetique")) // Moyenne des 24h
        .withColumn("nom_de_la_station", lit("Auber")) // On remet le nom manuellement

      val dfChateletReady = chatelet_final
        .agg(avg("avg(Indicateur_Pollution_Global)").as("Indicateur_Synthetique"))
        .withColumn("nom_de_la_station", lit("Châtelet les Halles"))

      val dfNationReady = nation_final
        .agg(avg("avg(Indicateur_Pollution_Global)").as("Indicateur_Synthetique"))
        .withColumn("nom_de_la_station", lit("Nation"))

      // 2. On fusionne
      val dfPatchwork = dfAuberReady.union(dfChateletReady).union(dfNationReady)
      dfPatchwork.show()


      //val dfDebug = auber_IG.withColumn("heure", hour(col("DATE/HEURE")))
      // On regarde la moyenne de CHAQUE sous-indice par heure
//      dfDebug.groupBy("heure")
//        .agg(
//          avg("Indicateur_Pollution_Global").as("Global"),
//
//          avg("NO2").as("Score_NO2"),     // Est-ce le diesel des travaux ?
//          avg("PM10").as("Score_PM10"),   // Est-ce la poussière des travaux ?
//          avg("CO2").as("Score_CO2"),     // Est-ce l'arrêt de la ventilation ?
//          avg("PM2_5").as("Score_PM2.5"),
//          count("*").as("Nb_Mesures")         // Y a-t-il très peu de données à 3h ?
//        )
//        .orderBy("heure")
//        .show(24)



//GraphX
// 1. Définition manuelle des séquences de stations (L'ordre est crucial)
      val brancheA1 = Seq("Saint-Germain-en-Laye", "Le Vésinet - Le Pecq", "Le Vésinet-Centre", "Chatou-Croissy", "Rueil-Malmaison", "Nanterre-Ville", "Nanterre-Université", "Nanterre-Préfecture")
      val brancheA3 = Seq("Cergy-Le Haut", "Cergy-Saint-Christophe", "Cergy-Préfecture", "Neuville-Université", "Conflans-Fin-d'Oise", "Achères-Ville", "Maisons-Laffitte", "Sartrouville", "Houilles-Carrières-sur-Seine", "Nanterre-Préfecture")
      val brancheA5 = Seq("Poissy", "Achères-Grand-Cormier", "Maisons-Laffitte") // Rejoint la A3

      val tronconCentral = Seq("Nanterre-Préfecture", "La Défense", "Charles de Gaulle - Etoile", "Auber", "Châtelet les Halles", "Gare de Lyon", "Nation", "Vincennes")

      val brancheA2 = Seq("Vincennes", "Fontenay-sous-Bois", "Nogent-sur-Marne", "Joinville-le-Pont", "Saint-Maur - Créteil", "Le Parc de Saint-Maur", "Champigny", "La Varenne - Chennevières", "Sucy - Bonneuil", "Boissy-Saint-Léger")
      val brancheA4 = Seq("Vincennes", "Val de Fontenay", "Neuilly-Plaisance", "Bry-sur-Marne", "Noisy-le-Grand - Mont d'Est", "Noisy - Champs", "Noisiel", "Lognes", "Torcy", "Bussy-Saint-Georges", "Val d'Europe", "Marne-la-Vallée - Chessy")

      // Liste de tous les tronçons
      val tousLesTroncons = Seq(brancheA1, brancheA3, brancheA5, tronconCentral, brancheA2, brancheA4)





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
    println("--- Bornes appliquées ---")
    boundsMap.foreach { case (k, v) =>
      println(f"$k%s : [${v._1}%.2f, ${v._2}%.2f]")
    }

    dfResultat
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

    println(s"Colonnes détectées. Somme des poids bruts : $sommePoids. Recalcul des poids en cours...")

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



  /**
   * Construit le Graphe orienté du RER A et injecte les données de pollution.
   * @param dfPollution Le DataFrame contenant "nom_de_la_station" et "Indicateur_Synthetique"
   * @param spark La session Spark active (implicite)
   * @return Un objet GraphX où les sommets contiennent (NomStation, ScorePollution)
   */

  def construireGrapheRERA(dfPollution: DataFrame)(implicit spark: SparkSession): Graph[(String, Double), String] = {

    import spark.implicits._

    // --- 1. DÉFINITION DE LA TOPOLOGIE (Séquencement) ---
    // On définit l'ordre des stations pour chaque branche
    val dataRER = Seq(
      ("RER A", "Nanterre-Préfecture", 1, "CENTRE"),
      ("RER A", "La Défense", 2, "CENTRE"),
      ("RER A", "Charles de Gaulle - Etoile", 3, "CENTRE"),
      ("RER A", "Auber", 4, "CENTRE"),
      ("RER A", "Châtelet les Halles", 5, "CENTRE"),
      ("RER A", "Gare de Lyon", 6, "CENTRE"),
      ("RER A", "Nation", 7, "CENTRE"),
      ("RER A", "Vincennes", 8, "CENTRE"),
      // Branche A1 (St Germain)
      ("RER A", "Saint-Germain-en-Laye", 1, "BRANCHE_A1"),
      ("RER A", "Le Vésinet - Le Pecq", 2, "BRANCHE_A1"),
      ("RER A", "Le Vésinet-Centre", 3, "BRANCHE_A1"),
      ("RER A", "Chatou-Croissy", 4, "BRANCHE_A1"),
      ("RER A", "Rueil-Malmaison", 5, "BRANCHE_A1"),
      ("RER A", "Nanterre-Ville", 6, "BRANCHE_A1"),
      ("RER A", "Nanterre-Université", 7, "BRANCHE_A1"),
      ("RER A", "Nanterre-Préfecture", 8, "BRANCHE_A1"), // Jonction
      // Branche A3 (Cergy / Poissy simplifiée)
      ("RER A", "Cergy-Le Haut", 1, "BRANCHE_A3"),
      ("RER A", "Cergy-Saint-Christophe", 2, "BRANCHE_A3"),
      ("RER A", "Cergy-Préfecture", 3, "BRANCHE_A3"),
      ("RER A", "Neuville-Université", 4, "BRANCHE_A3"),
      ("RER A", "Conflans-Fin-d'Oise", 5, "BRANCHE_A3"),
      ("RER A", "Achères-Ville", 6, "BRANCHE_A3"),
      ("RER A", "Maisons-Laffitte", 7, "BRANCHE_A3"),
      ("RER A", "Sartrouville", 8, "BRANCHE_A3"),
      ("RER A", "Houilles-Carrières-sur-Seine", 9, "BRANCHE_A3"),
      ("RER A", "Nanterre-Préfecture", 10, "BRANCHE_A3"), // Jonction
      // Branche A2 (Boissy)
      ("RER A", "Vincennes", 1, "BRANCHE_A2"), // Jonction
      ("RER A", "Fontenay-sous-Bois", 2, "BRANCHE_A2"),
      ("RER A", "Nogent-sur-Marne", 3, "BRANCHE_A2"),
      ("RER A", "Joinville-le-Pont", 4, "BRANCHE_A2"),
      ("RER A", "Saint-Maur - Créteil", 5, "BRANCHE_A2"),
      ("RER A", "Le Parc de Saint-Maur", 6, "BRANCHE_A2"),
      ("RER A", "Champigny", 7, "BRANCHE_A2"),
      ("RER A", "La Varenne - Chennevières", 8, "BRANCHE_A2"),
      ("RER A", "Sucy - Bonneuil", 9, "BRANCHE_A2"),
      ("RER A", "Boissy-Saint-Léger", 10, "BRANCHE_A2"),
      // Branche A4 (Marne-la-Vallée)
      ("RER A", "Vincennes", 1, "BRANCHE_A4"), // Jonction
      ("RER A", "Val de Fontenay", 2, "BRANCHE_A4"),
      ("RER A", "Neuilly-Plaisance", 3, "BRANCHE_A4"),
      ("RER A", "Bry-sur-Marne", 4, "BRANCHE_A4"),
      ("RER A", "Noisy-le-Grand - Mont d'Est", 5, "BRANCHE_A4"),
      ("RER A", "Noisy - Champs", 6, "BRANCHE_A4"),
      ("RER A", "Noisiel", 7, "BRANCHE_A4"),
      ("RER A", "Lognes", 8, "BRANCHE_A4"),
      ("RER A", "Torcy", 9, "BRANCHE_A4"),
      ("RER A", "Bussy-Saint-Georges", 10, "BRANCHE_A4"),
      ("RER A", "Val d'Europe", 11, "BRANCHE_A4"),
      ("RER A", "Marne-la-Vallée - Chessy", 12, "BRANCHE_A4")
    )

    val dfTopology = spark.createDataFrame(dataRER).toDF("ligne", "nom_station", "ordre", "segment")

    // Helper pour l'ID GraphX
    def hashId(str: String): Long = str.hashCode.toLong

    // --- 2. CONSTRUCTION DES LIENS (EDGES) ---
    val windowSpec = Window.partitionBy("segment").orderBy("ordre")

    val edgesRDD: RDD[Edge[String]] = dfTopology
      .withColumn("station_suivante", lead("nom_station", 1).over(windowSpec))
      .filter(col("station_suivante").isNotNull)
      .rdd
      .map { row =>
        val src = row.getAs[String]("nom_station")
        val dst = row.getAs[String]("station_suivante")
        Edge(hashId(src), hashId(dst), "suivante")
      }

    // --- 3. CONSTRUCTION DES NOEUDS (VERTICES) avec POLLUTION ---
    val uniqueStations = dfTopology.select("nom_station").distinct()

    // Jointure avec les données de pollution fournies
    val verticesRDD: RDD[(VertexId, (String, Double))] = uniqueStations
      .join(dfPollution, uniqueStations("nom_station") === dfPollution("nom_de_la_station"), "left_outer")
      .select(
        col("nom_station"),
        // Si on n'a pas de donnée pollution pour une station, on met 0.0 par défaut
        coalesce(col("Indicateur_Synthetique"), lit(0.0)).as("score_pollution")
      )
      .rdd
      .map { row =>
        val name = row.getAs[String]("nom_station")
        val score = row.getAs[Double]("score_pollution")
        (hashId(name), (name, score))
      }

    // --- 4. CRÉATION DU GRAPHE ---
    Graph(verticesRDD, edgesRDD)
  }

}

