package sk.vub.nifi

import java.io.OutputStream

import org.apache.nifi.components._
import org.apache.nifi.components.state.StateManager
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._

import scala.language.implicitConversions

package object ops {

  // PropertyDescriptor

  implicit def toPropertyDescriptorValue(pd: PropertyDescriptor)(implicit context: ProcessContext): PropertyDescriptorValue =
    new PropertyDescriptorValue(pd.getDisplayName, context.getProperty(pd))

  implicit def toPropertyDescriptorOps(pd: PropertyDescriptor): PropertyDescriptorOps =
    new PropertyDescriptorOps(pd)

  implicit def toPropertyValueOps(pv: PropertyValue): PropertyValueOps =
    new PropertyValueOps(pv)

  // OutputStream

  implicit def toOuputStreamOps(os: OutputStream): OutputStreamOps =
    new OutputStreamOps(os)

  // FlowFile

  implicit def toFlowFileOps(flowFile: FlowFile): FlowFileOps =
    new FlowFileOps(flowFile)

  implicit def toFlowFileExt(flowFile: FlowFile): FlowFileExt =
    new FlowFileExt(flowFile)

  // ProcessContext

  implicit def getStateManager(implicit context: ProcessContext): StateManager =
    context.getStateManager

  implicit def createNewProperty(value: String)(implicit context: ProcessContext): PropertyValue =
    context.newPropertyValue(value)

}
