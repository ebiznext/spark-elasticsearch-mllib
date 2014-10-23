package utils

import java.util.Date

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.DocumentSource
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.search.SearchHit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._


object EsClient {
  val settings = ImmutableSettings.settingsBuilder().put("cluster.name", Settings.ElasticSearch.Cluster).build()
  val client = ElasticClient.remote(settings, (Settings.ElasticSearch.Host, Settings.ElasticSearch.Port))

  def apply(): ElasticClient = client

  def index[T: Manifest](t: T, refresh: Boolean = true): String = {
    val json = JacksonConverter.serialize(t)
    val res = client.client.prepareIndex(Settings.ElasticSearch.Index, manifest[T].runtimeClass.getSimpleName)
      .setSource(json)
      .setRefresh(refresh)
      .execute()
      .actionGet()
    res.getId
  }

  def load[T: Manifest](uuid: String): Future[Option[T]] = {
    val req = get id uuid from Settings.ElasticSearch.Index -> manifest[T].runtimeClass.getSimpleName
    val res = client.execute(req)
    res map { res =>
      if (res.isExists) Some(JacksonConverter.deserialize[T](res.getSourceAsString)) else None
    }
  }

  def loadWithVersion[T: Manifest](uuid: String): Future[Option[(T, Long)]] = {
    val req = get id uuid from Settings.ElasticSearch.Index -> manifest[T].runtimeClass.getSimpleName
    val res = client.execute(req)
    res map { res =>
      val maybeT = if (res.isExists) Some(JacksonConverter.deserialize[T](res.getSourceAsString)) else None
      maybeT map ((_, res.getVersion))
    }
  }

  def delete[T: Manifest](uuid: String, refresh: Boolean): Future[Boolean] = {
    val req = com.sksamuel.elastic4s.ElasticDsl.delete id uuid from Settings.ElasticSearch.Index -> manifest[T].runtimeClass.getSimpleName refresh refresh
    val res = client.execute(req)
    res map { res =>
      res.isFound
    }
  }

  def update[T: Manifest](uuid: String, t: T, upsert: Boolean, refresh: Boolean): Future[Boolean] = {
    val js = JacksonConverter.serialize(t)
    val req = com.sksamuel.elastic4s.ElasticDsl.update id uuid in Settings.ElasticSearch.Index -> manifest[T].runtimeClass.getSimpleName refresh refresh doc new DocumentSource {
      override def json: String = js
    }
    req.docAsUpsert(upsert)
    val res = client.execute(req)
    res.map { res =>
      res.isCreated || res.getVersion > 1
    }
  }

  def updateWithVersion[T: Manifest](uuid: String, t: T, version: Long) = {
    val js = JacksonConverter.serialize(t)
    val req = com.sksamuel.elastic4s.ElasticDsl.update id uuid in Settings.ElasticSearch.Index -> manifest[T].runtimeClass.getSimpleName version version doc new DocumentSource {
      override def json: String = js
    }
    val res = client.execute(req)
    true
  }


  def searchAll[T: Manifest](req: SearchDefinition): Future[Seq[T]] = {
    val res = client.execute(req.size(Integer.MAX_VALUE))
    res.map { res =>
      res.getHits.getHits.map { hit => JacksonConverter.deserialize[T](hit.getSourceAsString)}
    }
  }

  def search[T: Manifest](req: SearchDefinition): Future[Option[T]] = {
    val res = client.execute(req.size(Integer.MAX_VALUE))
    res.map { res =>
      if (res.getHits.getTotalHits == 0)
        None
      else
        Some(JacksonConverter.deserialize[T](res.getHits.getHits()(0).getSourceAsString))
    }
  }

  def searchAllRaw(req: SearchDefinition): Future[Array[SearchHit]] = {
    val res = client.execute(req.size(Integer.MAX_VALUE))
    res.map { res =>
      res.getHits.getHits
    }
  }

  def searchRaw(req: SearchDefinition): Future[Option[SearchHit]] = {
    val res = client.execute(req.size(Integer.MAX_VALUE))
    res.map { res =>
      if (res.getHits.getTotalHits == 0)
        None
      else
        Some(res.getHits.getHits()(0))
    }
  }
  def esType[T:Manifest]: String = {
    val rt = manifest[T].runtimeClass
    rt.getSimpleName
  }

}
