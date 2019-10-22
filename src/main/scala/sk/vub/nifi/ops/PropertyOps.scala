package sk.vub.nifi.ops

import org.apache.nifi.components._
import org.apache.nifi.controller.ControllerService
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.ProcessContext

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

class PropertyDescriptorOps(val pd: PropertyDescriptor) extends AnyVal {

  def value(implicit context: ProcessContext): PropertyValue =
    context.getProperty(pd)

  def evaluate(flowFile: FlowFile)(implicit context: ProcessContext): PropertyDescriptorValue =
    new PropertyDescriptorValue(pd.getDisplayName, context.getProperty(pd).evaluateAttributeExpressions(flowFile))

  def evaluate(map: Map[String, String])(implicit context: ProcessContext): PropertyDescriptorValue =
    new PropertyDescriptorValue(pd.getDisplayName, context.getProperty(pd).evaluateAttributeExpressions(map.asJava))

  def evaluate(flowFile: FlowFile, map: Map[String, String])(implicit context: ProcessContext): PropertyDescriptorValue =
    new PropertyDescriptorValue(pd.getDisplayName, context.getProperty(pd).evaluateAttributeExpressions(flowFile, map.asJava))

  def evaluate(implicit context: ProcessContext): PropertyDescriptorValue =
    new PropertyDescriptorValue(pd.getDisplayName, context.getProperty(pd).evaluateAttributeExpressions())

}

class PropertyValueOps(val pv: PropertyValue) extends AnyVal {

  def evaluate(flowFile: FlowFile)(implicit context: ProcessContext): PropertyValue =
    pv.evaluateAttributeExpressions(flowFile)

  def get(implicit context: ProcessContext): String =
    pv.getValue

}

class PropertyDescriptorValue(name: String, prop: PropertyValue) {

  private def getValue[A](onValue: String => A, onUnset: => A, onNull: => A): A = {
    if (prop.isSet) {
      val value = prop.getValue
      if (value != null) onValue(value)
      else onNull
    } else onUnset
  }

  def getOpt: Option[String] =
    getValue(Some(_), None, None)

  def getEither: Either[String, String] =
    getValue(Right(_), Left(s"$name is not set"), Left(s"$name is null"))

  def get: String =
    getEither.fold(e => throw new NoSuchElementException(e), identity)

  def asControllerService[A <: ControllerService: ClassTag]: A =
    prop.asControllerService(implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]])

}
