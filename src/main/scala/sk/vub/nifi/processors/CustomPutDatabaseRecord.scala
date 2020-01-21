package sk.vub.nifi.processors

import java.sql._
import java.time.Instant
import java.time.format.DateTimeFormatter

import io.circe.parser.parse
import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.util.pattern.RollbackOnFailure
import org.apache.nifi.processor.{ProcessContext, ProcessSession, Relationship}
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.Record
import sk.vub.nifi.ops._
import sk.vub.nifi.processors.CustomPutDatabaseRecord._
import sk.vub.nifi.{FlowFileNotNull, ScalaProcessor, _}

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags(scala.Array("custom", "put", "database", "record"))
class CustomPutDatabaseRecord extends ScalaProcessor with FlowFileNotNull {

  def properties: List[PropertyDescriptor] = List(
    P.recordReader,
    P.dbpcService,
    P.mapping,
    P.sqlStatement,
    P.batchSize,
    P.queryTimeout,
    P.autoCommit
  )

  def relationships: Set[Relationship] = Set(R.success, R.failure)

  val format: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

  def setPreparedStatement(record: Record, name: String, value: String, index: Int, preparedStatement: PreparedStatement): Unit =
    try {
      value match {
        case "boolean" =>
          if (record.getAsBoolean(name) == null) preparedStatement.setNull(index, Types.BOOLEAN)
          else preparedStatement.setBoolean(index, record.getAsBoolean(name))
        case "convert-boolean-tinyint" =>
          if (record.getAsBoolean(name) == null) preparedStatement.setNull(index, Types.TINYINT)
          else {
            val res = if (record.getAsBoolean(name)) 1 else 0
            preparedStatement.setByte(index, res.toByte)
          }
        case "string" | "char" | "varchar" =>
          if (record.getAsString(name) == null) preparedStatement.setNull(index, Types.VARCHAR)
          else preparedStatement.setString(index, record.getAsString(name))
        case "tinyint" =>
          if (record.getAsInt(name) == null) preparedStatement.setNull(index, Types.TINYINT)
          else preparedStatement.setByte(index, record.getAsInt(name).toByte)
        case "smallint" =>
          if (record.getAsInt(name) == null) preparedStatement.setNull(index, Types.SMALLINT)
          else preparedStatement.setShort(index, record.getAsInt(name).toShort)
        case "int" =>
          if (record.getAsInt(name) == null) preparedStatement.setNull(index, Types.INTEGER)
          else preparedStatement.setInt(index, record.getAsInt(name))
        case "long" | "bigint" =>
          if (record.getAsLong(name) == null) preparedStatement.setNull(index, Types.BIGINT)
          else preparedStatement.setLong(index, record.getAsLong(name))
        case "double" =>
          if (record.getAsDouble(name) == null) preparedStatement.setNull(index, Types.DOUBLE)
          else preparedStatement.setDouble(index, record.getAsDouble(name))
        case "float" =>
          if (record.getAsFloat(name) == null) preparedStatement.setNull(index, Types.REAL)
          else preparedStatement.setFloat(index, record.getAsFloat(name))
        case "decimal" | "numberic" =>
          if (record.getAsString(name) == null) preparedStatement.setNull(index, Types.DECIMAL)
          else preparedStatement.setBigDecimal(index, new java.math.BigDecimal(record.getAsString(name)))
        case "date-timestamp" =>
          if (record.getAsLong(name) == null) preparedStatement.setNull(index, Types.DATE)
          else preparedStatement.setDate(index, new Date(record.getAsLong(name)))
        case "date-iso" =>
          if (record.getAsString(name) == null) preparedStatement.setNull(index, Types.DATE)
          else {
            preparedStatement.setDate(index, new Date(
              Instant.from(format.parse(record.getAsString(name))).toEpochMilli))
          }
        case "timestamp" =>
          if (record.getAsLong(name) == null) preparedStatement.setNull(index, Types.TIMESTAMP)
          else preparedStatement.setTimestamp(index, new Timestamp(record.getAsLong(name)))
        case "timestamp-iso" =>
          if (record.getAsString(name) == null) preparedStatement.setNull(index, Types.TIMESTAMP)
          else {
            preparedStatement.setTimestamp(index, new Timestamp(
              Instant.from(format.parse(record.getAsString(name))).toEpochMilli))
          }
        case _ =>
          throw new RuntimeException(s"Unsupported type in setPreparedStatement")
      }
    } catch {
      case e: RuntimeException =>
        getLogger.error("Error with setPreparedStatement", e)
        throw e
    }

  def addPreparedStatementToBatch(record: Record, preparedStatement: PreparedStatement, mapping: Seq[(String, String)]): Unit = {
    preparedStatement.clearParameters()
    for {
      ((name, value), index) <- mapping.zipWithIndex
    } {
      setPreparedStatement(record, name, value, index + 1, preparedStatement)
    }
    preparedStatement.addBatch()
  }

  case class FunctionContext(rollbackOnFailure: Boolean, queryTimeout: Int, originalAutoCommit: Boolean = false, jdbcUrl: String) {
    def roll = new RollbackOnFailure(rollbackOnFailure, true)
  }

  override def onTrigger(flowFile: FlowFile)(implicit context: ProcessContext, session: ProcessSession): Unit = {
    val readerFactory = P.recordReader.asControllerService[RecordReaderFactory]
    val dBCPService = P.dbpcService.asControllerService[DBCPService]

    val sqlStatement = P.sqlStatement.evaluate(flowFile).get
    val mapping: Seq[(String, String)] = parse(context
      .getProperty(P.mapping)
      .evaluateAttributeExpressions(flowFile)
      .getValue).getOrElse(throw new Exception("Invalid JSON format"))
      .as[Seq[Map[String, String]]].getOrElse(throw new Exception(s"Cannot parse property " +
      s"Mapping to JSON array")).map(_.head)
    val autoCommit = P.autoCommit.evaluate(flowFile).get.toBoolean
    val batchSize = P.batchSize.evaluate(flowFile).get.toInt
    val queryTimeout = P.queryTimeout.evaluate(flowFile).get.toInt

    var counter = 0
    withThrowableAsEither(()) { _ =>
      withResource(dBCPService.getConnection(flowFile.getAttributes)) { connection =>
        connection.setAutoCommit(autoCommit)
        withResource(connection.prepareStatement(sqlStatement)) { preparedStatement =>
          preparedStatement.setQueryTimeout(queryTimeout)
          withResource(flowFile.read()) { in =>
            withResource(readerFactory.createRecordReader(flowFile, in, logger)) { reader =>
              val batches: Iterator[Record]#GroupedIterator[Record] =
                Iterator
                  .continually(reader.nextRecord())
                  .takeWhile(_ != null)
                  .sliding(batchSize, batchSize)
              for {
                batch <- batches
              } {
                for {
                  record <- batch
                } {
                  addPreparedStatementToBatch(record, preparedStatement, mapping)
                  counter += 1
                }
                preparedStatement.executeBatch()
                if (!autoCommit) connection.commit()
              }
            }
          }
        }
      }
    } match {
      case Left(e) =>
        logger.error("errors", e)
        flowFile
          .putAttribute("errors", e.getMessage + e.getCause.getMessage)
          .transfer(R.failure)
      case Right(_) =>
        flowFile
          .putAttribute("processed", counter.toString)
          .transfer(R.success)
    }
  }
}

object CustomPutDatabaseRecord {
  object P {
    val recordReader: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("put-db-record-record-reader")
      .displayName("Record Reader")
      .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
      .identifiesControllerService(classOf[RecordReaderFactory])
      .required(true)
      .build()

    val dbpcService: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("put-db-record-dcbp-service")
      .displayName("Database Connection Pooling Service")
      .description("The Controller Service that is used to obtain a connection to the database for sending records.")
      .identifiesControllerService(classOf[DBCPService])
      .required(true)
      .build()

    val mapping: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("mapping")
      .description("Mapping of columns to database datatypes (as json array)")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .defaultValue("[]")
      .required(true)
      .build()

    val sqlStatement: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("sql-statement")
      .displayName("SQL Statement")
      .description("SQL Statement")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .defaultValue("INSERT INTO ${table}(ID, NAME) VALUES(?, ?)")
      .required(true)
      .build()

    val batchSize: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("batch-size")
      .displayName("Batch Size")
      .description("Batch Size")
      .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .defaultValue("10000")
      .required(true)
      .build()

    val queryTimeout: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("query-timeout")
      .displayName("Query Timeout")
      .description("Query Timeout in seconds")
      .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .defaultValue("300")
      .build()

    val autoCommit: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("auto-commit")
      .displayName("SQL auto commit")
      .description(
        """SQL auto commit commits after every row insert,
          | set to false to commit whole batches (better performance)""".stripMargin)
      .required(true)
      .allowableValues("true", "false")
      .defaultValue("true")
      .build()
  }

  object R {
    val success: Relationship = new Relationship.Builder()
      .name("success")
      .description("Successfully created FlowFile from SQL query result set.")
      .build()

    val failure: Relationship = new Relationship.Builder()
      .name("failure")
      .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
        + "such as an invalid query or an integrity constraint violation")
      .build()
  }
}
