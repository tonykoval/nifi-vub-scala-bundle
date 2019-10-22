package sk.vub.nifi

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.{AttributeExpression, ExpressionLanguageScope}
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators

import scala.jdk.CollectionConverters._

abstract class ScalaProcessor extends AbstractProcessor {

  def properties: List[PropertyDescriptor]

  def relationships: Set[Relationship]

  override def getSupportedPropertyDescriptors(): java.util.List[PropertyDescriptor] =
    properties.asJava

  override def getRelationships(): java.util.Set[Relationship] =
    relationships.asJava

  def logger: ComponentLog = getLogger

}

trait SupportDynamicProperties { this: ScalaProcessor =>

  override def getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String): PropertyDescriptor = {
    new PropertyDescriptor.Builder()
      .name(propertyDescriptorName)
      .required(false)
      .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
      .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .dynamic(true)
      .build()
  }
}

trait FlowFileNotNull { this: ScalaProcessor =>

  def onTrigger(flowFile: FlowFile)(implicit context: ProcessContext, session: ProcessSession): Unit

  def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    Option(session.get) match {
      case Some(flowFile) => onTrigger(flowFile)(context, session)
      case None => ()
    }
  }

}
