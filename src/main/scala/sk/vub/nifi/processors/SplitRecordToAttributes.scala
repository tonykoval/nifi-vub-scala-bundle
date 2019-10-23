package sk.vub.nifi.processors

import java.util.UUID

import io.circe.Json
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, WritesAttribute, WritesAttributes}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor.{ProcessContext, ProcessSession, Relationship}
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.Record
import sk.vub.nifi.ops._
import sk.vub.nifi.processors.SplitRecordToAttributes._
import sk.vub.nifi.{FlowFileNotNull, ScalaProcessor, _}

@Tags(Array("split", "record", "attribute", "json", "avro", "csv"))
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits up an input FlowFile that is in a record-oriented data format into multiple smaller " +
  "FlowFiles and content is also split into attributes as text. See examples in the additional details documentation " +
  "of this processor.")
@WritesAttributes(Array(
      new WritesAttribute(attribute = V.RecordCount, description = "The number of records in the FlowFile. This is " +
        "added to FlowFiles that are routed to the 'splits' Relationship."),
      new WritesAttribute(attribute = V.FragmentIndexKey, description = "All split FlowFiles produced from the same " +
        "parent FlowFile will have the same randomly generated UUID added for this attribute"),
      new WritesAttribute(attribute = V.FragmentIndexKey, description = "A one-up number that indicates the ordering " +
        "of the split FlowFiles that were created from a single parent FlowFile"),
      new WritesAttribute(attribute = V.FragmentCountKey, description = "The number of split FlowFiles generated from" +
        " the parent FlowFile"),
      new WritesAttribute(attribute = V.SegmentOriginalFilenameKey, description = "The filename of the parent FlowFile")
  )
)
class SplitRecordToAttributes extends ScalaProcessor with FlowFileNotNull {

  def properties: List[PropertyDescriptor] = List (P.recordReader, P.evaluateContent)

  def relationships: Set[Relationship] = Set(R.failure, R.original, R.splits)

  def onTrigger(flowFile: FlowFile)(implicit context: ProcessContext, session: ProcessSession): Unit = {
    val readerFactory = P.recordReader.asControllerService[RecordReaderFactory]
    val evaluateContent = P.evaluateContent.evaluate(flowFile).get.toBoolean
    val fragmentId = UUID.randomUUID().toString;

    withThrowableAsEither(()) { _ =>
      withResource(flowFile.read()) { in =>
        withResource(readerFactory.createRecordReader(flowFile, in, logger)) { reader =>
          val iterator: Iterator[Record] = Iterator.continually(reader.nextRecord()).takeWhile(_ != null)
          val records = iterator.toList
          val recordCount = records.size
          records.zipWithIndex.foreach { case (record, fragmentIndex) =>
            parseString(flattenJson(convertRecordToJsonString(logger, record))).asObject.foreach { jsonObject =>
              val attributes: Map[String, String] = (for {
                k <- jsonObject.keys
              } yield {
                if (evaluateContent) {
                  k -> context
                    .newPropertyValue(jsonToString(jsonObject(k).getOrElse(Json.Null)).getOrElse(""))
                    .evaluateAttributeExpressions(flowFile)
                    .getValue
                } else k -> jsonToString(jsonObject(k).getOrElse(Json.Null)).getOrElse("")
              }).toMap

              flowFile
                .createChild()
                .putAttribute(V.RecordCount, recordCount.toString)
                .putAttribute(V.FragmentIdKey, fragmentId)
                .putAttribute(V.FragmentCountKey, recordCount.toString)
                .putAttribute(V.FragmentIndexKey, fragmentIndex.toString)
                .putAttribute(V.SegmentOriginalFilenameKey, flowFile.getAttribute(CoreAttributes.FILENAME.key()))
                .putAllAttributes(attributes)
                .transfer(R.splits)
            }
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

object SplitRecordToAttributes {
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
      .description("Upon successfully splitting an input FlowFile, the original FlowFile will be sent to" +
        " this relationship.")
      .build()

    val splits: Relationship = new Relationship.Builder()
      .name("splits")
      .description("The individual 'segments' of the original FlowFile will be routed to this relationship.")
      .build()

    val failure: Relationship = new Relationship.Builder()
      .name("failure")
      .description("If a FlowFile cannot be transformed from the configured input format to the configured " +
        "output format, the unchanged FlowFile will be routed to this relationship.")
      .build()
  }

  object V {
    final val RecordCount = "record.count"
    final val FragmentIdKey = "fragment.identifier"
    final val FragmentIndexKey = "fragment.index"
    final val FragmentCountKey = "fragment.count"
    final val SegmentOriginalFilenameKey = "segment.original.filename"
  }
}
