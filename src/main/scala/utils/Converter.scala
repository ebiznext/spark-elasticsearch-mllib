package utils

import java.io.{BufferedOutputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.Array.canBuildFrom

/**
 * Generic Object Converter
 * Binary converter based on Java standard serializer
 * A performance improvement would be to rely on https://code.google.com/p/kryo/
 *
 * JSON converter based on jackson scala module
 */
trait Converter[T] {
  def toDomain[T: Manifest](obj: Array[Byte]): T

  def fromDomain[T: Manifest](value: T): Array[Byte]
}


trait BinaryConverter[T] extends Converter[T] {
  def toDomain[T: Manifest](obj: Array[Byte]): T = safeDecode(obj)

  def fromDomain[T: Manifest](value: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(new BufferedOutputStream(bos))
    out writeObject (value)
    out close()
    bos toByteArray()
  }

  def safeDecode[T: Manifest](bytes: Array[Byte]) = {
    val cl = Option(this.getClass().getClassLoader())
    val cin = cl match {
      case Some(cls) =>
        new CustomObjectInputStream(new ByteArrayInputStream(bytes), cls)
      case None =>
        new ObjectInputStream(new ByteArrayInputStream(bytes))
    }
    val obj = cin.readObject
    cin.close
    obj.asInstanceOf[T]
  }
}

trait JSONConverter[T] extends Converter[T] {
  def toDomain[T: Manifest](bytes: Array[Byte]): T = {
    val x: Option[T] = None
    JacksonConverter.deserialize[T](new String(bytes))
  }

  def fromDomain[T: Manifest](value: T): Array[Byte] = {
    JacksonConverter.serialize(value) map (_.toChar) toCharArray() map (_.toByte)
  }
}

object JacksonConverter {

  import java.lang.reflect._

  lazy val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def serialize(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def deserialize[T: Manifest](json: String): T = mapper.readValue(json, typeReference[T])

  private[this] def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType: Type = typeFromManifest(manifest[T])
  }

  private[this] def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) {
      m.runtimeClass
    }
    else new ParameterizedType {
      def getRawType = m.runtimeClass

      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray

      def getOwnerType = null
    }
  }
}
