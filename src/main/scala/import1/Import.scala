package import1

// imports - will be omitted for other examples


import java.io.File
import java.util.Date

import model.Model.Rating
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.action.bulk.BulkResponse
import utils.{EsClient, Settings}

import scala.util.control.NonFatal

object Import extends App {

  import com.sksamuel.elastic4s.ElasticDsl._
  import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse

  def deleteIndex: DeleteIndexResponse = {
    val deleteIndexDefinition = delete index ("scalaio2014")
    EsClient().execute(deleteIndexDefinition).await
  }

  import org.elasticsearch.action.admin.indices.create.CreateIndexResponse

  def createIndex: CreateIndexResponse = {
    import com.sksamuel.elastic4s.mappings.FieldType.{IntegerType, StringType}
    val createIndexDefinition = create index "scalaio2014" mappings(
      "Rating" as(
        "index" typed IntegerType,
        "userid" typed IntegerType,
        "movieid" typed IntegerType,
        "rating" typed IntegerType,
        "timestamp" typed IntegerType
        ),
      "User" as(
        "userid" typed IntegerType,
        "movieid" typed StringType
        )
      )
    EsClient().execute(createIndexDefinition).await
  }


  def bulkIndex[T <: {def toMap() : Map[String, Any]} : Manifest](items: List[T]): BulkResponse = {

    val indexDefinitions = items.map(item => index into s"${Settings.ElasticSearch.Index}/${EsClient.esType[T]}" fields (item.toMap()))
    val bulkDefinition = bulk(
      indexDefinitions: _*
    )
    println(bulkDefinition._builder.requests().toString)
    EsClient().execute(bulkDefinition).await
  }


  def sparkInit(): SparkContext = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf().setAppName(Settings.Spark.AppName).setMaster(Settings.Spark.Master)

    val sparkContext = new SparkContext(conf)

    val libdir = new File(Settings.Spark.JarPath)
    libdir.list().foreach(jar => sparkContext.addJar(Settings.Spark.JarPath + jar))
    sparkContext
  }

  def loadFileSpark() = {
    val sparkContext = sparkInit()

    val ratingLines = sparkContext.textFile(Settings.Spark.RatingsFile)

    var i = 0
    val ratings: RDD[Rating] = ratingLines.map { ratingLine =>
      val ratingFields = ratingLine.split("\\s+")
      i = i + 1
      Rating(i, ratingFields(0).toInt, ratingFields(1).toInt, ratingFields(2).toInt, ratingFields(3).toLong)
    } cache()

    ratings.foreach {
      rating => bulkIndex(List(rating))
    }

    sparkContext.stop()
  }

  def loadFileLocal(): Unit = {
    val lines = scala.tools.nsc.io.File(Settings.Spark.RatingsFile).lines()
    var i = 0
    val ratings = lines.map { ratingLine =>
      val ratingFields = ratingLine.split("\\s+")
      i = i + 1
      Rating(i, ratingFields(0).toInt, ratingFields(1).toInt, ratingFields(2).toInt, ratingFields(3).toLong)
    }
    ratings.foreach { rating =>
      bulkIndex(List(rating))
    }
  }

  def doItAll(loader: () => Unit): Unit = {
    try {
      deleteIndex

    }
    catch {
      case NonFatal(e) => e.printStackTrace();println("No index need to be deleted")
    }

    createIndex
    println("Index created")
    val start = new Date().getTime
    loader()
    val duration = (new Date().getTime - start) / 1000
    println(s"duration=$duration")
  }

  def doItAllLocal(): Unit = {
    doItAll(loadFileLocal)
  }

  def doItAllSpark(): Unit = {
    doItAll(loadFileSpark)
  }

  doItAllSpark()
}
