package sk.vub

import cats.Applicative
import cats.instances.vector._
import cats.syntax.functor._
import cats.syntax.traverse._
import io.circe._
import io.circe.optics.JsonTraversalPath
import io.circe.optics.all._
import monocle.Traversal

import scala.language.higherKinds

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

  def unfoldNotNull[A <: AnyRef](next: => A): Stream[A] = {
    val n = next
    if (n == null) Stream.empty
    else n #:: unfoldNotNull(next)
  }

  def unfoldWhile[A](hasNext: () => Boolean, getNext: () => A): Stream[A] = {
    if (hasNext()) getNext() #:: unfoldWhile(hasNext, getNext)
    else Stream.empty
  }

//  def convertRecordToJson(logger: ComponentLog, record: Record): String = {
//    val out = new ByteArrayOutputStream()
//    val writeJsonResult = new WriteJsonResult(logger, record.getSchema, new NopSchemaAccessWriter,
//      out, true, NullSuppression.NEVER_SUPPRESS, null, null, null, null)
//    writeJsonResult.writeRecord(record)
//    writeJsonResult.flush()
//    new String(out.toByteArray, StandardCharsets.UTF_8)
//  }
//
//  def convertJsonToRecord(logger: ComponentLog, json: String, schema: RecordSchema): Record = {
//    val jsonTreeRowRecordReader = new JsonTreeRowRecordReader(IOUtils.toInputStream(json, StandardCharsets.UTF_8),
//      logger, schema, null, null, null)
//    jsonTreeRowRecordReader.nextRecord()
//  }

  def resolveOptions[A](opts: Option[A]*): Option[A] = opts.find(_.isDefined).flatten

  def contains[A](item: A, opts: Option[A]*): Option[Boolean] = {
    val flatten = opts.flatten
    if (flatten.isEmpty) None
    else Some(flatten.contains(item))
  }

  def getOptSeqStringValueByKey(in: Json, key: String): Option[Seq[String]] = {
    in
      .findAllByKey(key)
      .find(!_.isNull)
      .map(x =>
        x.asArray
          .getOrElse(throw new Exception(s"Invalid type (expected array) $x"))
          .map(x => x.asString.getOrElse(throw new Exception(s"Invalid value: $x for key: $key")))
      )
  }

  def getOptStringValueByKey(in: Json, key: String): Option[String] = {
    in
      .findAllByKey(key)
      .find(!_.isNull)
      .map(x => x.asString.getOrElse(throw new Exception(s"Invalid value: $x for key: $key")))
  }

  def jsonToString(json: Json): Option[String] = {
    json match {
      case x if json.isNumber => x.asNumber.map(_.toString)
      case x if json.isString => x.asString
      case x if json.isBoolean => x.asBoolean.map(_.toString)
      case _ => None
    }
  }

  def getOptValueByKey(in: Json, key: String): Option[String] = {
    in
      .findAllByKey(key)
      .find(!_.isNull)
      .map(x => jsonToString(x).getOrElse(throw new Exception(s"Invalid value: $x for key: $key")))
  }

  def getStringValueByKey(in: Json, key: String): String = {
    getOptStringValueByKey(in, key).getOrElse(throw new Exception(s"Cannot find value for key: $key"))
  }

  def getSeqStringValueByKey(in: Json, key: String): Seq[String] = {
    getOptSeqStringValueByKey(in, key).getOrElse(throw new Exception(s"Cannot find value for key: $key"))
  }

  def getOptDoubleValueByKey(in: Json, key: String): Option[Double] = {
    in
      .findAllByKey(key)
      .find(!_.isNull)
      .map(x => x.asNumber.getOrElse(throw new Exception(s"Invalid value: $x for key: $key")).toDouble)
  }

  def getDoubleValueByKey(in: Json, key: String): Double = {
    getOptDoubleValueByKey(in, key).getOrElse(throw new Exception(s"Cannot find value for key: $key"))
  }

  def wrapStringJson(s: Option[String]): Json = {
    s.map(x => Json.fromString(x)).getOrElse(Json.Null)
  }

  def wrapLongJson(l: Option[Long]): Json = {
    l.map(x => Json.fromLong(x)).getOrElse(Json.Null)
  }

  def wrapDoubleJson(d: Option[Double]): Json = {
    d.map(x => Json.fromDoubleOrNull(x)).getOrElse(Json.Null)
  }

  def deepFilterByKey(p: String => Boolean) = JsonTraversalPath(new Traversal[Json, Json] {

    override def modifyF[F[_]](f: Json => F[Json])(s: Json)(implicit FZ: scalaz.Applicative[F]): F[Json] = {

      implicit val F: Applicative[F] = csApplicative(FZ)

      def go(s: Json): F[Json] = {
        s.fold(
          F.pure(s),
          b => F.pure(Json.fromBoolean(b)),
          n => F.pure(Json.fromJsonNumber(n)),
          s => F.pure(Json.fromString(s)),
          xs => xs.traverse(go).map(Json.fromValues),
          obj =>
            obj.toVector
              .traverse {
                case (key, value) =>
                  if (p(key)) f(value).map(key -> _)
                  else go(value).map(key -> _)
              }.map(fs => Json.fromJsonObject(JsonObject.fromIterable(fs))))
      }
      go(s)
    }
  })
}
