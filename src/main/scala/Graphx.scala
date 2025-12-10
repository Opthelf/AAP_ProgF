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
                        pollution: Map[Int, Double],
                      ) extends Serializable

object Graphx {
  val nomCol = "`avg(Indicateur_Pollution_Global)`"
  val auberVector = vectoriserStation(DataReader.auberdf, "Auber", nomCol)
  val chateletVector = vectoriserStation(DataReader.chateletdf, "Châtelet les Halles", nomCol)
  val nationVector = vectoriserStation(DataReader.nationdf, "Nation", nomCol)
  val franklinVector = vectoriserStation(DataReader.franklindf, "Franklin D. Roosevelt")
  val saintGermainVector = vectoriserStation(DataReader.saintgermaindf, "Saint-Germain-des-Prés")
  val unionVector = auberVector
    .union(chateletVector)
    .union(nationVector)
    .union(franklinVector)
    .union(saintGermainVector)
  //Conversion DataFrame -> RDD
  val colis = convertirVersRDD(unionVector)

  def main(args: Array[String]): Unit = {
    auberVector.show()
    val spark = DataReader.spark
    import spark.implicits._
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
    val edges = spark.sparkContext.parallelize(edgesList)


    // --- 3. Création des SOMMETS (Vertices) ---
    // On prend tous les noms de stations, on dédoublonne (ex: Vincennes apparait 3 fois), et on crée les sommets
    val nomsUniques = OrdreLigne.tousLesTroncons.flatten.distinct

    val vertices = spark.sparkContext.parallelize(nomsUniques.map { nom =>
      val id = hashId(nom)

      val ligneInfo = mapLignes.getOrElse(nom, "Inconnu")

      val stationInfo = StationInfo(
        nom = nom,
        ligne = ligneInfo,
        pollution = Map[Int, Double]()
      )
      (id, stationInfo)
    })


    val graphInitial = Graph(vertices, edges)
    val graphFinal = graphInitial.outerJoinVertices(colis) {
      case (id, stationInfo, Some(nouvelleMap)) =>
        // On met à jour l'objet StationInfo avec la Map reçue
        stationInfo.copy(pollution = nouvelleMap)

      case (id, stationInfo, None) =>
        stationInfo // Pas de changement
    }


  }
  def hashId(nom: String): Long = if (nom == null) 0L else nom.hashCode.toLong

  def convertirVersRDD(dfVectorise: DataFrame): RDD[(VertexId, Map[Int, Double])] = {

    // Helper ID (interne à la fonction pour être autonome)

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
      (hashId(nomStation), mapVector)
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

  def exporterVersGephi(graph: Graph[StationInfo, String], exportDir: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._ // Indispensable pour .toDF() et les $

    println(s" Démarrage de l'export vers : $exportDir")

    // --- 1. EXPORT DES NOEUDS (NODES) ---

    // A. Transformation RDD -> DataFrame
    val nodesDF = graph.vertices.map { case (id, info) =>
      (id, info.nom, info.ligne, info.pollution)
    }.toDF("Id", "Label", "Ligne", "MapPollution")

    // B. Création des 24 colonnes dynamiques
    val timeColumns = (0 to 23).map { h =>
      // On extrait la valeur pour l'heure h, ou 0.0 si vide
      coalesce(col("MapPollution").getItem(h), lit(0.0)).as(f"P_$h%02dh")
    }

    // C. Sélection finale (Colonnes Fixes + Colonnes Dynamiques)
    val fixedColumns = Seq(col("Id"), col("Label"), col("Ligne"))
    val allColumns = fixedColumns ++ timeColumns

    val finalNodesDF = nodesDF.select(allColumns: _*)

    // D. Écriture CSV
    println("   -> Écriture du fichier Nodes...")
    finalNodesDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(s"$exportDir/nodes")


    // --- 2. EXPORT DES ARÊTES (EDGES) ---

    val edgesDF = graph.edges.map { e =>
      (e.srcId, e.dstId, e.attr)
    }.toDF("Source", "Target", "Type")

    println("   -> Écriture du fichier Edges...")
    edgesDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(s"$exportDir/edges")

    println("✅ Export Gephi terminé avec succès !")
  }
}
