
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import Graphx.{exporterVersGephi, hashId}
import projet.DataReader
import projet.OrdreLigne


case class StationNode(
                          nom: String,
                          lignes: String,
                          pollution : Double
                        ) extends Serializable


object GraphIDF {

  def main(args: Array[String]): Unit = {
    val spark = DataReader.spark
    import spark.implicits._
    val dfPollution : DataFrame = DataReader.reseauidf_df
    println(dfPollution.distinct().count())
    val topologieReseau = OrdreLigne.definitionReseauMetro ++ OrdreLigne.definitionReseauRER

    val mapPollution = dfPollution
      .select(
        col("nom_de_la_station"),
        regexp_replace(col("pollution_air"), ",", ".").cast("double").as("score")
      )
      .na.fill(0.0) // Sécurité
      .groupBy("nom_de_la_station")
      .agg(max("score")) // Gestion des doublons
      .collect()
      .map(r => (r.getString(0).trim, r.getDouble(1)))
      .toMap

    val mapLignes = topologieReseau
      .flatMap { case (stations, nomLigne) => stations.map(s => (s, nomLigne)) }
      .groupBy(_._1) // Group By Station
      .map { case (station, list) =>
        (station, list.map(_._2).distinct.sorted.mkString(" / "))
      }
    val stationsUniques = topologieReseau.flatMap(_._1).distinct

    val verticesRDD: RDD[(VertexId, StationNode)] = spark.sparkContext.parallelize(
      stationsUniques.map { nom =>
        val id = hashId(nom)

        // On remplit la fiche signalétique de la station
        val info = StationNode(
          nom = nom,
          lignes = mapLignes.getOrElse(nom, "Inconnu"),
          pollution = mapPollution.getOrElse(nom, 0.0) // 0.0 si pas dans le DF
        )

        (id, info)
      }
    )
    val edgesList = topologieReseau.flatMap { case (stations, nomLigne) =>
      stations.sliding(2).flatMap { case Seq(src, dst) =>
        Seq(
          Edge(hashId(src), hashId(dst), nomLigne), // Aller
          Edge(hashId(dst), hashId(src), nomLigne)  // Retour
        )
      }
    }

    val edgesRDD = spark.sparkContext.parallelize(edgesList)

    // C. Assemblage
    val graph = Graph(verticesRDD, edgesRDD)
    exporterVersGephiGlobal(graph,"export_global")(spark)








  }
  def convertirVersRDD2(df: DataFrame): RDD[(VertexId, Double)] = {

    // Helper ID (interne à la fonction pour être autonome)
    def localHashId(str: String): Long = if (str == null) 0L else str.hashCode.toLong

    df.rdd.map { row =>
      // 1. Récupération et Nettoyage du Nom
      val nomStation = row.getString(0).trim()

      // 2. Récupération de la Map (Gestion des types Java/Scala)
      // On utilise le try/catch au cas où une ligne serait mal formée
      val score = try {
        if (row.isNullAt(1)) 0.0 else row.getAs[Double](1)

      } catch {
        case _: Throwable => 0.0 // Map vide si erreur
      }

      // 3. Création du tuple (ID, Donnée)
      (localHashId(nomStation), score)
    }
  }
  def exporterVersGephiGlobal(graph: Graph[StationNode, String], exportDir: String)(implicit spark: SparkSession): Unit = {

    import spark.implicits._

    println(s"💾 Démarrage de l'export GLOBAL vers : $exportDir")

    // --- 1. EXPORT DES NOEUDS (NODES) ---

    // Transformation simple : (ID, Nom, Ligne, Score)
    val nodesDF = graph.vertices.map { case (id, station) =>
      (id, station.nom, station.lignes, station.pollution)
    }.toDF("Id", "Label", "Ligne", "Pollution_Score")

    println("   -> Écriture du fichier Nodes...")
    nodesDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";") // Point-virgule pour Gephi/Excel
      .csv(s"$exportDir/nodes")


    // --- 2. EXPORT DES ARÊTES (EDGES) ---
    // (Identique à l'autre fonction, les arêtes ne changent pas)

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

    println(s"✅ Export terminé ! Vous pouvez ouvrir le dossier '$exportDir' dans Gephi.")
  }

}

