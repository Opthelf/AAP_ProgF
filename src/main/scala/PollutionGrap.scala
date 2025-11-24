import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import DataReader.reader
object PollutionGraph {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.graphx._
    import org.apache.spark.rdd.RDD

    // 1. Initialisation de la session Spark
    val spark = SparkSession.builder
      .appName("StationGraphAnalysis")
      .master("local[*]") // À adapter selon votre cluster
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR") // Pour réduire le bruit dans la console

    // 2. Chargement et Nettoyage des données
    // Remplacez le chemin ci-dessous par le chemin réel de votre fichier
    val pathToFile = "src/Data/le-reseau-de-transport-francilien.csv"
    val rawRdd = sc.textFile(pathToFile)

    // Séparation du header
    val header = rawRdd.first()
    val dataRdd = rawRdd.filter(row => row != header)

    // Définition d'une classe pour manipuler les données plus facilement
    // Index basés sur votre structure : 0:ID, 1:Nom, 2:Ligne, 4:Niveau_Pollution
    case class Station(id: String, nom: String, ligne: String, pollution: String)

    // Parsing du CSV
    val stationsRDD = dataRdd.flatMap { line =>
      val cols = line.split(";")
      if (cols.length > 4) {
        Some(Station(cols(0), cols(1), cols(2), cols(4)))
      } else {
        None // Gestion des lignes mal formées
      }
    }

    // 3. Création des Sommets (Vertices)
    // GraphX requiert un Long comme ID. Nous hachons l'ID string pour obtenir un Long.
    // Structure : (VertexId, (NomStation, NiveauPollution))
    val vertices: RDD[(VertexId, (String, String))] = stationsRDD.map { s =>
      (s.id.hashCode.toLong, (s.nom, s.pollution))
    }.distinct()

    // 4. Création des Arêtes (Edges)
    // On regroupe par ligne pour connecter les stations séquentiellement (A -> B -> C)
    val edges: RDD[Edge[String]] = stationsRDD
      .groupBy(_.ligne) // On regroupe toutes les stations d'une même ligne
      .flatMap { case (nomLigne, stationsIter) =>
        // Conversion en liste pour pouvoir ordonner et faire des fenêtres glissantes
        // NOTE: Idéalement, il faut trier cette liste par ordre de passage si vous avez une colonne 'ordre' ou 'sequence'.
        // Ici, on suppose l'ordre du fichier.
        val stationsList = stationsIter.toList

        // Sliding(2) crée des paires : (Station1, Station2), (Station2, Station3)...
        stationsList.sliding(2).flatMap { window =>
          if (window.size == 2) {
            val src = window(0)
            val dst = window(1)
            // Edge(SrcId, DstId, Attribut de l'arête (Nom de la ligne))
            Some(Edge(src.id.hashCode.toLong, dst.id.hashCode.toLong, nomLigne))
          } else {
            None
          }
        }
      }

    // 5. Construction du Graphe
    val graph = Graph(vertices, edges)

    // ---------------------------------------------------------
    // 6. VÉRIFICATION ET AFFICHAGE
    // ---------------------------------------------------------

    println("\n--- RÉSUMÉ DU GRAPHE ---")
    println(s"Nombre de stations (Sommets) : ${graph.vertices.count()}")
    println(s"Nombre de connexions (Arêtes) : ${graph.edges.count()}")

    println("\n--- EXEMPLE DE CONNEXIONS (TRIPLETS) ---")
    // Affiche : Station A --[Ligne]--> Station B (Niveau Pollution A / B)
    graph.triplets.take(10).foreach { triplet =>
      println(s"[${triplet.srcAttr._1}] est reliée à [${triplet.dstAttr._1}] " +
        s"via la ligne '${triplet.attr}'. " +
        s"(Pollution source: ${triplet.srcAttr._2})")
    }

    println("\n--- ANALYSE RAPIDE : Stations connectées très polluées ---")
    // Exemple de filtrage sur le graphe
    val subGraphPollution = graph.subgraph(vpred = (id, attr) => attr._2.toLowerCase.contains("élevé") || attr._2.toLowerCase.contains("high"))
    println(s"Nombre de stations 'polluées' dans le sous-graphe : ${subGraphPollution.vertices.count()}")
  }
}