package sk.vub.nifi.ops

import java.io._
import java.nio.file.Path
import java.util.regex.Pattern

import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._

import scala.jdk.CollectionConverters._

/** Provides convenience methods from ProcessSession related to FlowFile */
class FlowFileOps(val flowFile: FlowFile) extends AnyVal {

  def penalize()(implicit session: ProcessSession): FlowFile =
    session.penalize(flowFile)

  def putAttribute(key: String, value: String)(implicit session: ProcessSession): FlowFile =
    session.putAttribute(flowFile, key, value)

  def putAllAttributes(attributes: java.util.Map[String, String])(implicit session: ProcessSession): FlowFile =
    session.putAllAttributes(flowFile, attributes)

  def putAllAttributes(attributes: Map[String, String])(implicit session: ProcessSession): FlowFile =
    session.putAllAttributes(flowFile, attributes.asJava)

  def removeAttribute(key: String)(implicit session: ProcessSession): FlowFile =
    session.removeAttribute(flowFile, key)

  def removeAllAttributes(keys: java.util.Set[String])(implicit session: ProcessSession): FlowFile =
    session.removeAllAttributes(flowFile, keys)

  def removeAllAttributes(keys: Set[String])(implicit session: ProcessSession): FlowFile =
    session.removeAllAttributes(flowFile, keys.asJava)

  def removeAllAttributes(keyPattern: Pattern)(implicit session: ProcessSession): FlowFile =
    session.removeAllAttributes(flowFile, keyPattern)

  def transfer(relationship: Relationship)(implicit session: ProcessSession): Unit =
    session.transfer(flowFile, relationship)

  def transfer()(implicit session: ProcessSession): Unit =
    session.transfer(flowFile)

  def remove()(implicit session: ProcessSession): Unit =
    session.remove(flowFile)

  def read()(implicit session: ProcessSession): InputStream =
    session.read(flowFile)

  def exportTo(destination: Path, append: Boolean)(implicit session: ProcessSession): Unit =
    session.exportTo(flowFile, destination, append)

  def exportTo(destination: OutputStream)(implicit session: ProcessSession): Unit =
    session.exportTo(flowFile, destination)

  def createChild()(implicit session: ProcessSession): FlowFile =
    session.create(flowFile)

  def clone()(implicit session: ProcessSession): FlowFile =
    session.clone(flowFile)

  def clone(offset: Long, size: Long)(implicit session: ProcessSession): FlowFile =
    session.clone(flowFile, offset, size)

  def read(reader: InputStream => Unit)(implicit session: ProcessSession): Unit =
    session.read(flowFile, (in: InputStream) => reader(in))

  def read(allowSessionStreamManagement: Boolean, reader: InputStream => Unit)(implicit session: ProcessSession): Unit =
    session.read(flowFile, allowSessionStreamManagement, (in: InputStream) => reader(in))

  def write(stream: (InputStream, OutputStream) => Unit)(implicit session: ProcessSession): FlowFile =
    session.write(flowFile, (in: InputStream, out: OutputStream) => stream(in, out))

  def write(writer: OutputStream => Unit)(implicit session: ProcessSession): FlowFile =
    session.write(flowFile, (out: OutputStream) => writer(out))

  def write()(implicit session: ProcessSession): OutputStream =
    session.write(flowFile)

  def append(writer: OutputStream => Unit)(implicit session: ProcessSession): FlowFile =
    session.append(flowFile, (out: OutputStream) => writer(out))

  def importFrom(source: Path, keepSourceFile: Boolean)(implicit session: ProcessSession): FlowFile =
    session.importFrom(source, keepSourceFile, flowFile)

  def importFrom(source: InputStream)(implicit session: ProcessSession): FlowFile =
    session.importFrom(source, flowFile)

}
