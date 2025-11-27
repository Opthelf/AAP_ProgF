package projet
import projet.DataReader
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, DecisionTreeRegressor, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object Prediction {
  def main(args: Array[String]): Unit = {

    // --- ÉTAPE 1 : ÉTIQUETAGE & FUSION ---
    // On ajoute une colonne "nom_station" pour identifier la source
    val spark = DataReader.spark
    val auberml = DataReader.auberml
    val chateletml = DataReader.chateletml
    val nationml = DataReader.nationml
    val franklinml = DataReader.franklinml
    val saintgermainml = DataReader.saintgermainml

    val df1 = auberml.withColumn("nom_station", lit("Auber"))
    val df2 = chateletml.withColumn("nom_station", lit("Chatelet"))
    val df3 = nationml.withColumn("nom_station", lit("Nation"))
    val df4 = franklinml.withColumn("nom_station", lit("Franklin"))
    val df5 = saintgermainml.withColumn("nom_station", lit("StGermain"))
    val seqStation = Seq(df1,df2,df3,df4,df5)

    val dfGlobal = df2

    seqStation.foreach { currentDF =>
      val nomStation = currentDF.select("nom_station").first().getString(0)
      println(s"\n" + "="*50)
      println(s"  🚉  TRAITEMENT DE : ${nomStation.toUpperCase}")
      println(s"="*50)

      val windowSpec = Window.orderBy("DATE/HEURE")
      val dfFeatures = currentDF
        .withColumn("heure_jour", hour(col("DATE/HEURE")))
        .withColumn("mois", month(col("DATE/HEURE")))
        .withColumn("jour_semaine", dayofweek(col("DATE/HEURE")))
        .withColumn("pollution_precedente", lag("Indicateur_Pollution_Global", 1).over(windowSpec))
        .na.drop() // On supprime les premières lignes qui n'ont pas d'historique
        // On nettoie les trous (la toute première ligne n'a pas de "précédent", elle devient null)
        .na.drop()

      val assembler = new VectorAssembler()
        .setInputCols(Array("heure_jour", "mois", "jour_semaine","pollution_precedente"))
        .setOutputCol("features")

      val final_df = assembler.transform(dfFeatures)
        .withColumnRenamed("Indicateur_Pollution_Global", "label")

      val Array(trainData, testData) = final_df.randomSplit(Array(0.8, 0.2), seed = 43L)

      println(s"Données d'entraînement : ${trainData.count()} lignes")
      println(s"Données de test : ${testData.count()} lignes")

      // 3. ENTRAÎNEMENT DES MODÈLES

      // --- Modèle A : Régression Linéaire (La base) ---
      println("\n--- 1. Régression Linéaire ---")
      val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features")
      val lrModel = lr.fit(trainData)
      val lrPred = lrModel.transform(testData)

      // --- Modèle B : Arbre de Décision (Capable de voir les pics non-linéaires) ---
      println("--- 2. Arbre de Décision ---")
      val dt = new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("features")
      val dtModel = dt.fit(trainData)
      val dtPred = dtModel.transform(testData)

      // --- Modèle C : Forêt Aléatoire (Le plus robuste) ---
      println("--- 3. Forêt Aléatoire ---")
      val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features").setNumTrees(50)
      val rfModel = rf.fit(trainData)
      val rfPred = rfModel.transform(testData)

      // 4. ÉVALUATION (Comparaison des RMSE)
      val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

      val rmseLR = evaluator.evaluate(lrPred)
      val rmseDT = evaluator.evaluate(dtPred)
      val rmseRF = evaluator.evaluate(rfPred)

      println("\n=== 🏆 RÉSULTATS (Erreur Moyenne) ===")
      println(f"Régression Linéaire : $rmseLR%.4f")
      println(f"Arbre de Décision   : $rmseDT%.4f")
      println(f"Forêt Aléatoire     : $rmseRF%.4f")

      // 5. EXEMPLE DE PRÉDICTION CONCRÈTE
      // On regarde ce que la Forêt prédit pour une des lignes de test
      println("\n--- Exemple Réel vs Prédiction (Forêt) ---")
      rfPred.select("heure_jour", "label", "prediction").show(20)


    }
    // --- ÉTAPE 2 : FEATURE ENGINEERING AVANCÉ ---

    // A. Encodage du nom de la station (String -> Index Numérique)
    // L'IA a besoin de chiffres. "Auber" devient 0.0, "Nation" devient 1.0...






    //val dfService = final_df.filter("heure_jour = 15")


  }



}