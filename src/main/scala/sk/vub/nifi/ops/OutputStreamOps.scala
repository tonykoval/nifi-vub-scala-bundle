package sk.vub.nifi.ops

import java.io.OutputStream
import java.nio.charset.Charset

class OutputStreamOps(val os: OutputStream) {

  def write(s: String)(implicit charset: Charset): Unit =
    os.write(s.getBytes(charset))

}
