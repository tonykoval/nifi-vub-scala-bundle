package sk.vub

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import com.github.wnameless.json.flattener.{FlattenMode, JsonFlattener}
import io.circe.Json
import io.circe.jawn._
import org.apache.commons.text.StringEscapeUtils
import org.apache.nifi.json.WriteJsonResult
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.record.NullSuppression
import org.apache.nifi.schema.access.NopSchemaAccessWriter
import org.apache.nifi.serialization.record.Record

package object nifi {

  def withThrowableAsEither[A, R](a: A)(f: A => R): Either[Throwable, R] = {
    try {
      Right(f(a))
    } catch {
      case e: Throwable => Left(e)
    }
  }

  def withResource[A <: AutoCloseable, R](a: A)(f: A => R): R = {
    try {
      f(a)
    } finally {
      a.close()
    }
  }

  def assertNotNull[A <: AnyRef](a: A): A = {
    assert(a != null, "value was null")
    a
  }

  def resolveOptions[A](opts: Option[A]*): Option[A] = opts.find(_.isDefined).flatten

  def contains[A](item: A, opts: Option[A]*): Option[Boolean] = {
    val flatten = opts.flatten
    if (flatten.isEmpty) None
    else Some(flatten.contains(item))
  }

  def convertRecordToJsonString(logger: ComponentLog, record: Record): String = {
    val out = new ByteArrayOutputStream()
    val writeJsonResult = new WriteJsonResult(logger, record.getSchema, new NopSchemaAccessWriter,out, true,
      NullSuppression.NEVER_SUPPRESS, null, null, null, null)
    writeJsonResult.writeRecord(record)
    writeJsonResult.flush()
    new String(out.toByteArray, StandardCharsets.UTF_8)
  }

  def flattenJson(json: String, separator: Char = '.', flattenMode: FlattenMode = FlattenMode.NORMAL): String = {
    new JsonFlattener(json)
      .withFlattenMode(flattenMode)
      .withSeparator(separator)
      .withStringEscapePolicy(() => StringEscapeUtils.ESCAPE_JSON)
      .flatten()
  }

  def convertRecordToJson(logger: ComponentLog, record: Record): Json = {
    parse(convertRecordToJsonString(logger, record)).getOrElse(throw new RuntimeException("invalid json"))
  }

  def parseString(in: String): Json = {
    parse(in).getOrElse(throw new RuntimeException("invalid json"))
  }

  def jsonToString(json: Json): Option[String] = {
    json match {
      case x if json.isNumber => x.asNumber.map(_.toString)
      case x if json.isString => x.asString
      case x if json.isBoolean => x.asBoolean.map(_.toString)
      case x if json.isArray => x.asArray.map(_.mkString("[", ",", "]"))
      case _ => None
    }
  }
}
