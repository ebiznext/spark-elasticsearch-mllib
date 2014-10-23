package colfilt2

import java.io.File

import com.sksamuel.elastic4s.ElasticDsl._
import import1.Import
import model.Model.{Rating, User}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import utils.{EsClient, Settings}

object CollaborativeFiltering extends App {
  def sparkInit(): SparkContext = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName(Settings.Spark.AppName).setMaster(Settings.Spark.Master)
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "19200")
    val sparkContext = new SparkContext(conf)

    val libdir = new File(Settings.Spark.JarPath)
    libdir.list().foreach(jar => sparkContext.addJar(Settings.Spark.JarPath + jar))
    sparkContext
  }


  def summary(): Unit = {
    import org.elasticsearch.spark._
    val sparkContext = sparkInit()
    val esType = s"${Settings.ElasticSearch.Index}/${EsClient.esType[Rating]}"
    val ratings = sparkContext.esRDD(esType)
    val users = ratings.map(_._2("userid")).distinct
    val nbUsers = users.count
    val nbItems = ratings.map(_._2("itemid")).distinct.count
    println("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
    println(s"$nbUsers users rated $nbItems")
    println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

    //    val res: Seq[String] = rdd.map(_._2("itemid").toString).countByValue().toSeq.sortBy(-_._2).take(50).map(_._1)
    //    res.foreach(println)

    sparkContext.stop()
  }

  def als() = {
    import org.elasticsearch.spark._
    val sparkContext = sparkInit()
    val esType = s"${Settings.ElasticSearch.Index}/${EsClient.esType[Rating]}"
    val esWildcardQuery =  search in Settings.ElasticSearch.Index -> EsClient.esType[Rating] query { matchall }

    val ratings = sparkContext.esRDD(esType)

    import org.apache.spark.mllib.recommendation.Rating

    val allData = sparkContext.esRDD(esType, esWildcardQuery._builder.toString).cache()

    val allDataCount = allData.count()

    val trainingSet = allData.filter { x =>
      val index = x._2("index").toString.toLong
      index % 10 != 1 && index % 10 != 2
    }.map(x => Rating(x._2("userid").toString.toInt, x._2("itemid").toString.toInt, x._2("rating").toString.toDouble)).cache()

    val validatingSet = allData.filter { x =>
      val index = x._2("index").toString.toLong
      index % 10 == 1
    }.map(x => Rating(x._2("userid").toString.toInt, x._2("itemid").toString.toInt, x._2("rating").toString.toDouble)).cache()

    val testingSet = allData.filter { x =>
      val index = x._2("index").toString.toLong
      index % 10 == 2
    }.map(x => Rating(x._2("userid").toString.toInt, x._2("itemid").toString.toInt, x._2("rating").toString.toDouble)).cache()

    val numTraining = trainingSet.count()
    val numValidation = validatingSet.count()
    val numTest = testingSet.count()



    val ranks = List(12)
    val lambdas = List(0.16)
    val numIters = List(30)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

    def rmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
      import org.apache.spark.SparkContext._
      val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
      val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating)).join(data.map(x => ((x.user, x.product), x.rating))).values
      math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
    }

    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(trainingSet, rank, numIter, lambda)
      val validationRmse = rmse(model, validatingSet)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")

      println(trainingSet.count() + "////" + testingSet.count() + "////" + validatingSet.count())

      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    val testRmse = rmse(bestModel.get, testingSet)

    val users = trainingSet.map(_.user).distinct.collect()

    //println("================" + users.length)

    val usersRecommendations = users.map { userId =>
      val recommendations = bestModel.get.recommendProducts(userId, 10).map(_.product)
      User(userId, recommendations)
    }

    Import.bulkIndex(usersRecommendations.toList)

    import org.apache.spark.SparkContext._
    val meanRating = trainingSet.union(validatingSet).map(_.rating).mean()

    val baselineRmse =
      math.sqrt(testingSet.map((x: Rating) => (meanRating - x.rating) * (meanRating - x.rating)).mean)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    println("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
    println("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
    println("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")
    println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

    sparkContext.stop()

  }

  als()
  Thread.sleep(30000)
}
