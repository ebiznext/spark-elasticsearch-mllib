package utils

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  val Env = if (System.getenv.containsKey("PRODUCTION")) {
    Environment.PROD
  } else {
    Environment.DEV
  }

  object ElasticSearch {
    val DateFormat = config.getString("elasticsearch.date.format")
    val Host = config.getString("elasticsearch.host")
    val HttpPort = config.getInt("elasticsearch.http.port")
    val Port = config.getInt("elasticsearch.port")
    val Index = config.getString("elasticsearch.index")
    val Cluster = config.getString("elasticsearch.cluster")
    val FullUrl = Host + ":" + HttpPort
    println("ElascticSearch on " + Host + ":" + Port + ",index->" + Index + ", cluster->" + Cluster)
  }

  object Spark {
    val Master = config.getString("spark.master")
    val AppName = config.getString("spark.appName")
    val DataPath = config.getString("spark.dataPath")
    val RatingsFile = DataPath + config.getString("spark.ratingsFile")
    val ItemsFile = DataPath + config.getString("spark.itemsFile")
    val JarPath= config.getString("spark.jarPath")

  }

}

object Environment extends Enumeration {
  type Environment = Value
  val DEV = Value(1)
  val PROD = Value(2)
}
