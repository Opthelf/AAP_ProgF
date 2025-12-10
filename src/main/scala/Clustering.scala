import projet.DataReader
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.sql.functions._
object Clustering {
  val data = DataReader.reseauidf_cluster
  def main(args: Array[String]): Unit = {
    val spark = DataReader.spark
    val dfClusteringRaw = data
      .select(
        col("nom_de_la_station"),
        col("nom_de_la_ligne"),
        // On nettoie les chiffres (virgules -> points)
        regexp_replace(col("stop_lat"), ",", ".").cast("double").as("lat"),
        regexp_replace(col("stop_lon"), ",", ".").cast("double").as("lon"),
        regexp_replace(col("pollution_air"), ",", ".").cast("double").as("pollution")
      )
      .na.drop() // On supprime les lignes avec des trous

    val assembler = new VectorAssembler()
      .setInputCols(Array("lat", "lon", "pollution"))
      .setOutputCol("features_raw")

    val scaler = new StandardScaler()
      .setInputCol("features_raw")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    // On applique le pipeline
    val dfVectorized = assembler.transform(dfClusteringRaw)
    val scalerModel = scaler.fit(dfVectorized)
    val dfReady = scalerModel.transform(dfVectorized)

    println(s"Données prêtes pour ${dfReady.count()} stations.")

    // --- 3. ENTRAÎNEMENT DU K-MEANS ---
    // On choisit K=2 (pour essayer de trouver 2 types de zones)
    val kmeans = new KMeans().setK(2).setSeed(42L).setFeaturesCol("features")
    val model = kmeans.fit(dfReady)

    // On fait les prédictions (On attribue un n° de cluster à chaque station)
    val predictions = model.transform(dfReady)

    // --- 4. ANALYSE DES RÉSULTATS ---

    println("\n---  Analyse des Clusters (Zones détectées) ---")

    // On calcule la moyenne de pollution et la position moyenne par cluster
    predictions.groupBy("prediction")
      .agg(
        count("*").as("nb_stations"),
        avg("pollution").as("pollution_moyenne"),
        avg("lat").as("lat_moyenne"), // Pour savoir où c'est
        avg("lon").as("lon_moyenne")
      )
      .orderBy("pollution_moyenne")
      .show(false)

    val hashIdUDF = udf((nom: String) => if (nom == null) 0L else nom.trim.hashCode.toLong)






  }

}