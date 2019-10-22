package sk.vub.nifi.processors

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor.{ProcessContext, ProcessSession, Relationship}
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.Record
import sk.vub.nifi.ops._
import sk.vub.nifi.{FlowFileNotNull, ScalaProcessor, _}
import sk.vub.nifi.processors.SplitRecordsToAttributes._

import scala.jdk.CollectionConverters._

@Tags(Array("split", "record", "attributes"))
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Split Record to Attributes")
class SplitRecordsToAttributes extends ScalaProcessor with FlowFileNotNull {

  def properties: List[PropertyDescriptor] = List (P.recordReader, P.evaluateContent)

  def relationships: Set[Relationship] = Set(R.failure, R.original, R.splits)

  def onTrigger(flowFile: FlowFile)(implicit context: ProcessContext, session: ProcessSession): Unit = {
    val readerFactory = P.recordReader.asControllerService[RecordReaderFactory]
    val evaluateContent = P.evaluateContent.evaluate(flowFile).get.toBoolean

    withThrowableAsEither(()) { _ =>
      withResource(flowFile.read()) { in =>
        withResource(readerFactory.createRecordReader(flowFile, in, logger)) { reader =>
          val iterator: Iterator[Record] = Iterator.continually(reader.nextRecord()).takeWhile(_ != null)
          for {
            record <- iterator
          } {
            val attributes: Map[String, String] = (for {
              rawField <- record.getRawFieldNames.asScala
            } yield {
              if (evaluateContent) {
                rawField -> context
                  .newPropertyValue(record.getAsString(rawField))
                  .evaluateAttributeExpressions(flowFile)
                  .getValue
              } else rawField -> record.getAsString(rawField)
            }).toMap

            flowFile
              .createChild()
              .putAllAttributes(attributes)
              .putAttribute(CoreAttributes.FILENAME.key, System.nanoTime().toString)
              .transfer(R.splits)
          }
        }
      }
    } match {
      case Left(e) =>
        flowFile
          .putAttribute("errors", e.toString)
          .transfer(R.failure)
      case Right(_) =>
        flowFile.transfer(R.original)
    }
  }
}

object SplitRecordsToAttributes {
  object P {
    val recordReader: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("record-reader")
      .displayName("Record Reader")
      .description("Specifies the Controller Service to use for reading incoming data")
      .identifiesControllerService(classOf[RecordReaderFactory])
      .required(true)
      .build()

    val evaluateContent: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("evaluate-content")
      .displayName("Evaluate Content")
      .description("Evaluate Content")
      .allowableValues("true", "false")
      .defaultValue("false")
      .required(true)
      .build()
  }

  object R {
    val original: Relationship = new Relationship.Builder()
      .name("original")
      .build()

    val splits: Relationship = new Relationship.Builder()
      .name("splits")
      .build()

    val failure: Relationship = new Relationship.Builder()
      .name("failure")
      .build()
  }
}
