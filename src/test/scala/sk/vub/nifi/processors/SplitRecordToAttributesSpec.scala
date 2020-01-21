package sk.vub.nifi.processors

import org.apache.nifi.json.JsonTreeReader
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.util.TestRunners
import org.scalatest.{FunSpec, Matchers}
import sk.vub.nifi.processors.SplitRecordToAttributes._

import scala.collection.JavaConverters._

class SplitRecordToAttributesSpec extends FunSpec with Matchers {

  it("json - one simple record") {
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

    val processor = new SplitRecordToAttributes
    val runner = TestRunners.newTestRunner(processor)

    val readerService = new MockRecordParser

    runner.addControllerService("reader", readerService)
    runner.enableControllerService(readerService)
    runner.setValidateExpressionUsage(false)

    val jsonReader = new JsonTreeReader
    runner.addControllerService("reader", jsonReader)
    runner.enableControllerService(jsonReader)

    runner.setProperty(P.recordReader, "reader")

    runner.enqueue(in)
    runner.run()

    runner.assertTransferCount(R.splits, 1)
    runner.assertTransferCount(R.failure, 0)
    runner.assertTransferCount(R.original, 1)

    for (flowFile <- runner.getFlowFilesForRelationship(R.splits).asScala) {
      flowFile.assertAttributeEquals("id", "123")
      flowFile.assertAttributeEquals("date", "2017-07-07")
      flowFile.assertAttributeEquals("boolean", "true")
      flowFile.assertAttributeEquals("arrays[0]", "1")
      flowFile.assertAttributeEquals("arrays[1]", "2")
      flowFile.assertAttributeEquals("arrays[2]", "3")
      flowFile.assertAttributeEquals("null", "")
      flowFile.assertAttributeEquals("record.count", "1")
      flowFile.assertAttributeEquals("fragment.count", "1")
      flowFile.assertAttributeEquals("fragment.index", "0")
    }
  }

  it("json - two simple records") {
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

    val processor = new SplitRecordToAttributes
    val runner = TestRunners.newTestRunner(processor)

    val readerService = new MockRecordParser

    runner.addControllerService("reader", readerService)
    runner.enableControllerService(readerService)
    runner.setValidateExpressionUsage(false)

    val jsonReader = new JsonTreeReader
    runner.addControllerService("reader", jsonReader)
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
      flowFile.assertAttributeExists("arrays[0]")
      flowFile.assertAttributeExists("arrays[1]")
      flowFile.assertAttributeExists("arrays[2]")
      flowFile.assertAttributeExists("null")
      flowFile.assertAttributeEquals("record.count", "2")
      flowFile.assertAttributeEquals("fragment.count", "2")
    }
  }

  it("json - nested object") {
    val in: String =
      """
        |[{
        |	"id": 1,
        |	"name": "John Doe",
        |	"address": "123 My Street",
        |	"city": "My City",
        |	"state": "MS",
        |	"zipCode": "11111",
        |	"country": "USA",
        |	"accounts": [{
        |		"id": 42,
        |		"balance": 4750.89
        |	}, {
        |		"id": 43,
        |		"balance": 48212.38
        |	}]
        |}]
        |""".stripMargin

    val processor = new SplitRecordToAttributes
    val runner = TestRunners.newTestRunner(processor)

    val readerService = new MockRecordParser

    runner.addControllerService("reader", readerService)
    runner.enableControllerService(readerService)
    runner.setValidateExpressionUsage(false)

    val jsonReader = new JsonTreeReader
    runner.addControllerService("reader", jsonReader)
    runner.enableControllerService(jsonReader)

    runner.setProperty(P.recordReader, "reader")

    runner.enqueue(in)
    runner.run()

    runner.assertTransferCount(R.splits, 1)
    runner.assertTransferCount(R.failure, 0)
    runner.assertTransferCount(R.original, 1)

    for (flowFile <- runner.getFlowFilesForRelationship(R.splits).asScala) {
      flowFile.assertAttributeEquals("id", "1")
      flowFile.assertAttributeEquals("name", "John Doe")
      flowFile.assertAttributeEquals("address", "123 My Street")
      flowFile.assertAttributeEquals("city", "My City")
      flowFile.assertAttributeEquals("state", "MS")
      flowFile.assertAttributeEquals("zipCode", "11111")
      flowFile.assertAttributeEquals("country", "USA")
      flowFile.assertAttributeEquals("accounts[0].id", "42")
      flowFile.assertAttributeEquals("accounts[0].balance", "4750.89")
      flowFile.assertAttributeEquals("accounts[1].id", "43")
      flowFile.assertAttributeEquals("accounts[1].balance", "48212.38")
      flowFile.assertAttributeEquals("record.count", "1")
      flowFile.assertAttributeEquals("fragment.count", "1")
      flowFile.assertAttributeEquals("fragment.index", "0")
    }
  }
}
