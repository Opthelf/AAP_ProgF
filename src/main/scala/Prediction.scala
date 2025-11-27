package projet
import projet.DataReader
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, DecisionTreeRegressor, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object Prediction {
  def main(args: Array[String]): Unit = {

    val spark = DataReader.spark
    val auberml = DataReader.auberml
    auberml.filter("Indicateur_Pollution_Global > 1").show()

    val windowSpec = Window.orderBy("DATE/HEURE")

    val dfEnhanced = auberml
      .withColumn("heure_jour", hour(col("DATE/HEURE")))
      .withColumn("mois", month(col("DATE/HEURE")))
      .withColumn("jour_semaine", dayofweek(col("DATE/HEURE"))) // Bonus : Lundi vs Dimanche
      // C'est ici qu'on crée la colonne magique :
      .withColumn("pollution_precedente", lag("Indicateur_Pollution_Global", 1).over(windowSpec))

      // On nettoie les trous (la toute première ligne n'a pas de "précédent", elle devient null)
      .na.drop()

    val assembler = new VectorAssembler()
      .setInputCols(Array("heure_jour", "mois", "jour_semaine", "TEMP", "HUMI", "pollution_precedente"))
      .setOutputCol("features")

    val final_df = assembler.transform(dfEnhanced)
      .withColumnRenamed("Indicateur_Pollution_Global", "label")
    println(final_df.count())
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
    rfPred.select("heure_jour", "TEMP", "label", "prediction").show(20)
  }

}