package sk.vub.nifi.ops

import java.nio.charset.Charset

import org.apache.commons.io.IOUtils
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._

/** Extends FlowFile with new convenience methods */
class FlowFileExt(val flowFile: FlowFile) extends AnyVal {

  def contentAsString(implicit session: ProcessSession, charset: Charset): String = {
    var result: String = null
    flowFile.read(in => result = IOUtils.toString(in, charset))
    result
  }

}
