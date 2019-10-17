package sk.vub.nifi.processors

import org.apache.nifi.json.JsonTreeReader
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.util.TestRunners
import org.scalatest.{FunSpec, Matchers}

import sk.vub.nifi.processors.SplitRecordsToAttributes._

import scala.collection.JavaConverters._

class SplitRecordToAttributesSpec extends FunSpec with Matchers {

  it("simple test") {
    val in: String =
      """
        |[
        |{
        |  "id": 123,
        |  "date": "2017-07-07",
        |  "boolean": true,
        |  "arrays": [1,2,3],
        |  "null": null
        |}
        |]
      """.stripMargin

    val inputSchemaText =
      """
        |{
        |  "type" : "record",
        |  "name" : "MyClass",
        |  "namespace" : "com.test.avro",
        |  "fields" : [ {
        |    "name" : "id",
        |    "type" : "long"
        |  }, {
        |    "name" : "date",
        |    "type" : "string"
        |  }, {
        |    "name" : "boolean",
        |    "type" : "boolean"
        |  }, {
        |    "name" : "arrays",
        |    "type" : {
        |      "type" : "array",
        |      "items" : "long"
        |    }
        |  }, {
        |    "name" : "null",
        |    "type" : [ "string", "null" ]
        |  } ]
        |}
      """.stripMargin

    val processor = new SplitRecordsToAttributes
    val runner = TestRunners.newTestRunner(processor)

    val readerService = new MockRecordParser

    runner.addControllerService("reader", readerService)
    runner.enableControllerService(readerService)
    runner.setValidateExpressionUsage(false)

    val jsonReader = new JsonTreeReader
    runner.addControllerService("reader", jsonReader)
    runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY)
    runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText)
    runner.enableControllerService(jsonReader)

    runner.setProperty(P.recordReader, "reader")
    runner.setProperty(P.evaluateContent, "false")

    runner.enqueue(in)
    runner.run()

    runner.assertTransferCount(R.splits, 1)
    runner.assertTransferCount(R.failure, 0)
    runner.assertTransferCount(R.original, 1)

    for (flowFile <- runner.getFlowFilesForRelationship(R.splits).asScala) {
      flowFile.assertAttributeEquals("id", "123")
      flowFile.assertAttributeEquals("date", "2017-07-07")
      flowFile.assertAttributeEquals("boolean", "true")
      flowFile.assertAttributeEquals("arrays", "[1, 2, 3]")
      flowFile.assertAttributeEquals("null", null)
    }
  }

  it("simple test 2") {
    val in: String =
      """
        |[
        |{
        |  "id": 123,
        |  "date": "2017-07-07",
        |  "boolean": true,
        |  "arrays": [1,2,3],
        |  "null": null
        |},
        |{
        |  "id": 456,
        |  "date": "2016-06-06",
        |  "boolean": false,
        |  "arrays": [4,5,6],
        |  "null": "not null"
        |}
        |]
      """.stripMargin

    val inputSchemaText =
      """
        |{
        |  "type" : "record",
        |  "name" : "MyClass",
        |  "namespace" : "com.test.avro",
        |  "fields" : [ {
        |    "name" : "id",
        |    "type" : "long"
        |  }, {
        |    "name" : "date",
        |    "type" : "string"
        |  }, {
        |    "name" : "boolean",
        |    "type" : "boolean"
        |  }, {
        |    "name" : "arrays",
        |    "type" : {
        |      "type" : "array",
        |      "items" : "long"
        |    }
        |  }, {
        |    "name" : "null",
        |    "type" : [ "string", "null" ]
        |  } ]
        |}
      """.stripMargin

    val processor = new SplitRecordsToAttributes
    val runner = TestRunners.newTestRunner(processor)

    val readerService = new MockRecordParser

    runner.addControllerService("reader", readerService)
    runner.enableControllerService(readerService)
    runner.setValidateExpressionUsage(false)

    val jsonReader = new JsonTreeReader
    runner.addControllerService("reader", jsonReader)
    runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY)
    runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText)
    runner.enableControllerService(jsonReader)

    runner.setProperty(P.recordReader, "reader")

    runner.enqueue(in)
    runner.run()

    runner.assertTransferCount(R.splits, 2)
    runner.assertTransferCount(R.failure, 0)
    runner.assertTransferCount(R.original, 1)

    for (flowFile <- runner.getFlowFilesForRelationship(R.splits).asScala) {
      flowFile.assertAttributeExists("id")
      flowFile.assertAttributeExists("date")
      flowFile.assertAttributeExists("boolean")
      flowFile.assertAttributeExists("arrays")
      flowFile.assertAttributeExists("null")
    }
  }
}
