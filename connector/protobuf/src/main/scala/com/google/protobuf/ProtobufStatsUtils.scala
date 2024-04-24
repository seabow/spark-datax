package com.google.protobuf

import com.google.protobuf.Descriptors.FieldDescriptor
object ProtobufStatsUtils {
  def computeFieldSize(field:FieldDescriptor,value: AnyRef):Int={
    com.google.protobuf.FieldSet.computeFieldSize(field,value)
  }
}
