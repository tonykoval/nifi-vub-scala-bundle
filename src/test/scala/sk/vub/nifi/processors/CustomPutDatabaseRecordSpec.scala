package sk.vub.nifi.processors

import java.io.{File, IOException}
import java.sql.{Connection, DriverManager, SQLNonTransientConnectionException}

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.json.JsonTreeReader
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.util.file.FileUtils
import org.apache.nifi.util.{TestRunner, TestRunners}
import org.mockito.Mockito.spy
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import sk.vub.nifi.processors.CustomPutDatabaseRecord._

import scala.collection.JavaConverters._

class CustomPutDatabaseRecordSpec extends FunSpec with Matchers with BeforeAndAfterAll {
  val DB_LOCATION = "target/db2"
  System.setProperty("derby.stream.error.file", "target/derby.log")

  override def beforeAll(): Unit = {
    try {
      val dbLocation = new File(DB_LOCATION)
      FileUtils.deleteFile(dbLocation, true)
    } catch {
      case _: IOException => // Do nothing, may not have existed
      case e: Exception => throw e
    }
  }

  override def afterAll(): Unit = {
    try {
      DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true")
      val dbLocation = new File(DB_LOCATION)
      FileUtils.deleteFile(dbLocation, true)
    } catch {
      case _: SQLNonTransientConnectionException => // Do nothing, this is what happens at Derby shutdown
      case _: IOException => // Do nothing, may not have existed
      case e: Exception => throw e
    }
  }

  class DBCPServiceSimpleImpl extends AbstractControllerService with DBCPService {
    override def getIdentifier: String = "dbcp"

    override def getConnection: Connection = {
      try {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
        DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true")
      } catch {
        case e: Exception => throw new ProcessException("getConnection failed: " + e);
      }
    }
  }

  def createTable(stmt: String)(implicit runner: TestRunner): Unit = {
    val con: Connection = runner.getControllerService("dbcp").asInstanceOf[DBCPService].getConnection
    con.setAutoCommit(false)
    val statement = con.createStatement()
    statement.execute(stmt)
    con.commit()
    con.close()
  }

  def getRowCount(table: String)(implicit runner: TestRunner): Int = {
    val con: Connection = runner.getControllerService("dbcp").asInstanceOf[DBCPService].getConnection
    con.setAutoCommit(false)
    val statement = con.createStatement()
    val resultSet = statement.executeQuery(s"SELECT COUNT(*) FROM $table")
    if (resultSet.next()) {
      val count = resultSet.getLong(1).toInt
      con.commit()
      con.close()
      count
    } else {
      throw new Exception(s"COUNT from $table failed")
    }
  }

  it("simple insert") {
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
        |  "date": "2017-07-08",
        |  "boolean": false,
        |  "arrays": [1,2,3],
        |  "null": "not-null"
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

    val mapping =
      """
        |[
        |{"id": "long"},
        |{"date": "string"}
        |]
      """.stripMargin

    val processor = new CustomPutDatabaseRecord
    implicit val runner: TestRunner = TestRunners.newTestRunner(processor)

    val dbcp: DBCPServiceSimpleImpl = spy(new DBCPServiceSimpleImpl())
    val dbcpProperties: Map[String, String] = Map.empty[String, String]

    runner.addControllerService("dbcp", dbcp, dbcpProperties.asJava)
    runner.enableControllerService(dbcp)

    val table = "TEST_INSERT"
    createTable(s"create table $table(id integer not null, date varchar(100))")
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
    runner.setProperty(P.dbpcService, "dbcp")
    runner.setProperty(P.mapping, mapping)
    runner.setProperty(P.sqlStatement, "INSERT INTO TEST_INSERT(id, date) VALUES(?, ?)")

    runner.enqueue(in)
    runner.run()

    runner.assertTransferCount(R.success, 1)
    runner.assertTransferCount(R.failure, 0)

    for (flowFile <- runner.getFlowFilesForRelationship(R.success).asScala) {
      flowFile.assertAttributeEquals("processed", "2")
    }

    getRowCount(table) should be(2)
  }

  it("decimal") {
    val inputSchemaText =
      """{
        |  "type": "record",
        |  "name": "NiFi_ExecuteSQL_Record",
        |  "namespace": "any.data",
        |  "fields": [
        |    {
        |      "name": "EDW_BUSINESS_DATE",
        |      "type": [
        |        "null",
        |        "string"
        |      ]
        |    },
        |    {
        |      "name": "ID_PARTY",
        |      "type": [
        |        "null",
        |        "string"
        |      ]
        |    },
        |    {
        |      "name": "VL_MAX_DRAWN_KTK_1M",
        |      "type": [
        |        "null",
        |        "string"
        |      ]
        |    },
        |    {
        |      "name": "VL_MAX_DRAWN_CARDS_1M",
        |      "type": [
        |        "null",
        |        "string"
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    val in1 =
      """|{
         |  "EDW_BUSINESS_DATE" : "1517439600000",
         |  "ID_PARTY" : "10122900072",
         |  "VL_MAX_DRAWN_KTK_1M" : "10.5",
         |  "VL_MAX_DRAWN_CARDS_1M" : null
         |}""".stripMargin

    val in2 =
      """|{
         |  "EDW_BUSINESS_DATE" : "1517439600000",
         |  "ID_PARTY" : "10122900073",
         |  "VL_MAX_DRAWN_KTK_1M" : 10.5,
         |  "VL_MAX_DRAWN_CARDS_1M" : null
         |}""".stripMargin

    val mapping =
      """|[
         |  {
         |    "EDW_BUSINESS_DATE": "date-timestamp"
         |  },
         |  {
         |    "ID_PARTY": "bigint"
         |  },
         |  {
         |    "VL_MAX_DRAWN_KTK_1M": "decimal"
         |  },
         |  {
         |    "VL_MAX_DRAWN_CARDS_1M": "decimal"
         |  }
         |]""".stripMargin

    val processor = new CustomPutDatabaseRecord
    implicit val runner: TestRunner = TestRunners.newTestRunner(processor)

    val dbcp: DBCPServiceSimpleImpl = spy(new DBCPServiceSimpleImpl())
    val dbcpProperties: Map[String, String] = Map.empty[String, String]

    runner.addControllerService("dbcp", dbcp, dbcpProperties.asJava)
    runner.enableControllerService(dbcp)

    val table = "TEST_INSERT_DECIMAL"
    createTable(
      s"""CREATE TABLE $table
         |(
         |  EDW_BUSINESS_DATE date NOT NULL,
         |  ID_PARTY bigint NOT NULL,
         |  VL_MAX_DRAWN_KTK_1M decimal(15,2),
         |  VL_MAX_DRAWN_CARDS_1M decimal(15,2),
         |  CONSTRAINT CONSTRAINT_6 PRIMARY KEY (EDW_BUSINESS_DATE, ID_PARTY)
         |)""".stripMargin)
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
    runner.setProperty(P.dbpcService, "dbcp")
    runner.setProperty(P.mapping, mapping)
    runner.setProperty(P.sqlStatement,
      s"""INSERT INTO $table(EDW_BUSINESS_DATE,ID_PARTY,VL_MAX_DRAWN_KTK_1M,VL_MAX_DRAWN_CARDS_1M) VALUES (?,?,?,?)""")

    runner.enqueue(in1)
    runner.enqueue(in2)
    runner.run(2)

    runner.assertTransferCount(R.success, 2)
    runner.assertTransferCount(R.failure, 0)

    getRowCount(table) should be(2)
  }

}
