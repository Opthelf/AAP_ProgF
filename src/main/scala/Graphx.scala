import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import projet.DataReader
import projet.OrdreLigne

case class StationInfo(
                        nom: String,
                        ligne: String,
                        pollution: Map[Int, Double], // Votre vecteur 24h
                      ) extends Serializable
object Graphx {
  def main(args: Array[String]): Unit = {
    val spark = DataReader.spark
    import spark.implicits._
    val nomCol = "`avg(Indicateur_Pollution_Global)`"
    val auberVector = vectoriserStation(DataReader.auberdf, "Auber", nomCol)
    val chateletVector = vectoriserStation(DataReader.chateletdf, "Châtelet les Halles", nomCol)
    val nationVector = vectoriserStation(DataReader.nationdf, "Nation", nomCol)
    val franklinVector = vectoriserStation(DataReader.franklindf, "Franklin D. Roosevelt")
    val saintGermainVector = vectoriserStation(DataReader.saintgermaindf, "Saint-Germain-des-Prés")
    val idfVector = DataReader.reseauidf_df
    val unionVector = auberVector
      .union(chateletVector)
      .union(nationVector)
      .union(franklinVector)
      .union(saintGermainVector)

    //Conversion DataFrame -> RDD

    val colis = convertirVersRDD(unionVector)




    // 1. Définition manuelle des séquences de stations (L'ordre est crucial)

    // Liste de tous les tronçons

    val mapLignes = OrdreLigne.definitionReseau
      .flatMap { case (stations, nomLigne) =>
        stations.map(station => (station, nomLigne))
      }
      .groupBy(_._1)
      .map { case (station, listPairs) =>
        val ligneStr = listPairs.map(_._2).distinct.sorted.mkString(" / ")
        (station, ligneStr)
      }
    // --- 2. Création des ARÊTES (Edges) ---
    // --- TRANSFORMATION EN ARÊTES (BIDIRECTIONNEL) ---

    val edgesList = OrdreLigne.tousLesTroncons.flatMap { ligne =>
      // On parcourt la ligne par paires (Station A, Station B)
      ligne.sliding(2).flatMap { case Seq(src, dst) =>
        Seq(
          Edge(hashId(src), hashId(dst), "connexion"),
          Edge(hashId(dst), hashId(src), "connexion")
        )
      }
    }
    val edgesRDD = spark.sparkContext.parallelize(edgesList)


    // --- 3. Création des SOMMETS (Vertices) ---
    // On prend tous les noms de stations, on dédoublonne (ex: Vincennes apparait 3 fois), et on crée les sommets
    val nomsUniques = OrdreLigne.tousLesTroncons.flatten.distinct

    val verticesRDD = spark.sparkContext.parallelize(nomsUniques.map { nom =>
      val id = hashId(nom)

      val ligneInfo = mapLignes.getOrElse(nom, "Inconnu")

      val stationInfo = StationInfo(
        nom = nom,
        ligne = ligneInfo,
        pollution = Map[Int, Double]()
      )
      (id, stationInfo)
    })


    val graphInitial = Graph(verticesRDD, edgesRDD)

    // Petite vérification
    val graphFinal = graphInitial.outerJoinVertices(colis) {
      case (id, stationInfo, Some(nouvelleMap)) =>
        // On met à jour l'objet StationInfo avec la Map reçue
        stationInfo.copy(pollution = nouvelleMap)

      case (id, stationInfo, None) =>
        stationInfo // Pas de changement
    }

    println("--- 📊 Affichage des Stations (Mode Objet) ---")

    // On ajoute .sortBy(...) pour ne pas avoir les stations dans le désordre
    graphFinal.vertices.collect().sortBy(_._2.nom).foreach { case (id, info) =>

      val nomStation = info.nom
      val mapDonnees = info.pollution

      // On crée une barre visuelle ou un statut
      val statut = if (mapDonnees.nonEmpty) s"✅ ACTIF (${mapDonnees.size}h)" else "⚪ INACTIF"

      // Affichage aligné
      println(f"Station : $nomStation%-30s | $statut")

      // Si on a des données, on affiche le détail pour 4h du matin
      if (mapDonnees.nonEmpty) {
        val pollution4h = mapDonnees.getOrElse(4, 0.0)
        // On affiche un petit avertissement visuel selon le niveau
        val niveau = if (pollution4h > 0.5) "⚠️ Élevé" else "✅ Bas"
        println(f"   -> 🕓 Pollution à 04h00 : $pollution4h%.3f ($niveau)")
      }
    }
    // Dossier de sortie
    val exportDir = "export_gephi_final"

    // --- 1. EXPORT DES NOEUDS (Stations avec les 24h) ---

    // On transforme le RDD de sommets en DataFrame pour faciliter l'export CSV avec Header
    val nodesDF = graphFinal.vertices.map { case (id, info) =>
      // On prépare une ligne avec : ID, Nom, Ligne + les 24 heures
      (id, info.nom, info.ligne, info.pollution)
    }.toDF("Id", "Label", "Ligne", "MapPollution")

    // L'astuce : On éclate la Map en 24 colonnes distinctes
    // On crée la liste des colonnes à sélectionner dynamiquement
    val timeColumns = (0 to 23).map { h =>
      // Pour chaque heure, on va chercher la valeur dans la Map. Si vide -> 0.0
      coalesce(col("MapPollution").getItem(h), lit(0.0)).as(f"P_$h%02dh")
    }

    // On sélectionne les colonnes fixes + les 24 colonnes dynamiques
    // 1. On définit les colonnes fixes
    val fixedColumns = Seq(col("Id"), col("Label"), col("Ligne"))

    // 2. On fusionne avec les colonnes dynamiques (opérateur ++)
    val allColumns = fixedColumns ++ timeColumns

    // 3. On passe le tout au select
    val finalNodesDF = nodesDF.select(allColumns: _*)

    println("--- Export des Nœuds (Nodes) ---")
    finalNodesDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(f"$exportDir/nodes")


    // --- 2. EXPORT DES ARÊTES (Rails) ---

    val edgesDF = graphFinal.edges.map { e =>
      (e.srcId, e.dstId, e.attr)
    }.toDF("Source", "Target", "Type")

    println("--- Export des Arêtes (Edges) ---")
    edgesDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(f"$exportDir/edges")

    println(s"✅ Export terminé ! Dossier : $exportDir")

















  }
  def hashId(nom: String): Long = if (nom == null) 0L else nom.hashCode.toLong

  def convertirVersRDD(dfVectorise: DataFrame): RDD[(VertexId, Map[Int, Double])] = {

    // Helper ID (interne à la fonction pour être autonome)
    def localHashId(str: String): Long = if (str == null) 0L else str.hashCode.toLong

    dfVectorise.rdd.map { row =>
      // 1. Récupération et Nettoyage du Nom
      val nomStation = row.getString(0).trim()

      // 2. Récupération de la Map (Gestion des types Java/Scala)
      // On utilise le try/catch au cas où une ligne serait mal formée
      val mapVector = try {
        row.getMap[Int, Double](1).toMap
      } catch {
        case _: Throwable => Map[Int, Double]() // Map vide si erreur
      }

      // 3. Création du tuple (ID, Donnée)
      (localHashId(nomStation), mapVector)
    }
  }

  /**
   * Transforme un DataFrame temporel (24 lignes) en un DataFrame vectorisé (1 ligne avec Map).
   * * @param df Le DataFrame source (doit contenir "heure").
   * @param nomStation Le nom de la station à assigner (ex: "Auber").
   * @param colScore Le nom exact de la colonne contenant le score (ex: "avg(Indicateur...)").
   * @return Un DataFrame avec 2 colonnes : "nom_de_la_station" et "profil_24h".
   */
  def vectoriserStation(df: DataFrame, nomStation: String, colScore: String = "`avg(Indicateur_Pollution_Global)`"): DataFrame = {
    df
      .withColumn("nom_de_la_station", lit(nomStation))
      .groupBy("nom_de_la_station")
      .agg(
        map_from_arrays(
          collect_list(col("heure")),
          collect_list(col(colScore))
        ).as("profil_24h")
      )
  }
}
