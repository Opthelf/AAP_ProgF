import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, max, min}
import org.apache.spark.sql.functions._
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
    val nomCol = "`avg(Indicateur_Pollution_Global)`"
    val auberVector = vectoriserStation(DataReader.auberdf,"Auber",nomCol)
    val chateletVector = vectoriserStation(DataReader.chateletdf,"Châtelet les Halles",nomCol)
    val nationVector = vectoriserStation(DataReader.nationdf,"Nation",nomCol)
    val franklinVector = vectoriserStation(DataReader.franklindf,"Franklin-D.Roosevelt")
    val saintGermainVector = vectoriserStation(DataReader.saintgermaindf,"Saint-Germain-des-Près")
    val unionVector = auberVector
      .union(chateletVector)
      .union(nationVector)

    //Conversion DataFrame -> RDD

    val colis = convertirVersRDD(unionVector)


//    dfAuberVector
//      .select(col("profil_24h")(19).as("Pollution_a_19h"))
//      .show()

    // 1. Définition manuelle des séquences de stations (L'ordre est crucial)

    // Liste de tous les tronçons


    // --- 2. Création des ARÊTES (Edges) ---
    // --- TRANSFORMATION EN ARÊTES (BIDIRECTIONNEL) ---

    val edgesList = OrdreLigne.rer_A.flatMap { ligne =>
      // On parcourt la ligne par paires (Station A, Station B)
      ligne.sliding(2).flatMap { case Seq(src, dst) =>
        val idA = hashId(src)
        val idB = hashId(dst)

        // Pour chaque paire, on crée DEUX arêtes inverses
        Seq(
          Edge(idA, idB, "connexion"), // Sens Aller
          Edge(idB, idA, "connexion") // Sens Retour
        )
      }
    }

    val edgesRDD = spark.sparkContext.parallelize(edgesList.toSeq)


    // --- 3. Création des SOMMETS (Vertices) ---
    // On prend tous les noms de stations, on dédoublonne (ex: Vincennes apparait 3 fois), et on crée les sommets
    val verticesList = OrdreLigne.rer_A.flatten.distinct.map { nom =>
      (hashId(nom), nom) // (ID, Propriété) -> Ici la propriété est juste le Nom pour l'instant
    }
    val verticesRDD: RDD[(Long, String)] = spark.sparkContext.parallelize(verticesList)

    // --- 4. Création du Graphe ---
    val graphRERA = Graph(verticesRDD, edgesRDD)

    val graphInitial = graphRERA.mapVertices{ (id, nomStation) =>
      StationInfo(
        nom = nomStation,
        ligne = "RER A",
        pollution = Map[Int, Double]()
      )
    }
    val graphFinal = graphInitial.outerJoinVertices(colis) {
      case (id, stationInfoExistante, Some(nouveauVecteur)) =>
        stationInfoExistante.copy(pollution = nouveauVecteur)
      case (id, stationInfoExistante, None)=>
        stationInfoExistante
    }
//    println("\n--- Vérification du Graphe Final ---")
//
//    // On cherche Auber
//    val auber = graphFinal.vertices
//      .filter { case (id, info) => info.nom == "Auber" }
//      .first()
//      ._2 // On récupère l'objet StationInfo

//    println(s"Station : ${auber.nom}")
//    println(s"Ligne   : ${auber.ligne}")
//    println(s"Données : ${auber.pollution.size} heures enregistrées")
//    println(s"Pollution à 19h : ${auber.pollution.getOrElse(4, 0.0)}")
//    println("--- Affichage des Stations (Mode Objet) ---")

    println("\n--- 🎲 Échantillon de connexions (Aller/Retour) ---")

    // On prend 10 connexions au hasard dans le paquet
    graphFinal.triplets.take(10).foreach { t =>
      println(s"🚄 ${t.srcAttr.nom}  -->  ${t.dstAttr.nom}")
    }
    println("--- Affichage des Stations (Mode Objet) ---")

    graphFinal.vertices.collect().foreach { case (id, info) =>
      // 'info' est maintenant votre objet StationInfo

      val nomStation = info.nom
      val mapDonnees = info.pollution

      // Affichage
      val statut = if (mapDonnees.nonEmpty) s"✅ ${mapDonnees.size} h de données" else "⚪ Vide"
      println(f"Station : $nomStation%-30s | $statut")

      if (mapDonnees.nonEmpty) {
        println(s"   -> Pollution à 4h : ${mapDonnees.getOrElse(4, 0.0)}")
      }

    }











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
