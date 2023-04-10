/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connect.common

import java.lang.{Boolean => JBoolean, Byte => JByte, Character => JChar, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time._

import scala.collection.mutable
import scala.reflect.ClassTag

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

object LiteralValueProtoConverter {

  /**
   * Transforms literal value to the `proto.Expression.Literal.Builder`.
   *
   * @return
   *   proto.Expression.Literal.Builder
   */
  @scala.annotation.tailrec
  def toLiteralProtoBuilder(literal: Any): proto.Expression.Literal.Builder = {
    val builder = proto.Expression.Literal.newBuilder()

    def decimalBuilder(precision: Int, scale: Int, value: String) = {
      builder.getDecimalBuilder.setPrecision(precision).setScale(scale).setValue(value)
    }

    def calendarIntervalBuilder(months: Int, days: Int, microseconds: Long) = {
      builder.getCalendarIntervalBuilder
        .setMonths(months)
        .setDays(days)
        .setMicroseconds(microseconds)
    }

    def arrayBuilder(array: Array[_]) = {
      val ab = builder.getArrayBuilder
        .setElementType(toConnectProtoType(toDataType(array.getClass.getComponentType)))
      array.foreach(x => ab.addElements(toLiteralProto(x)))
      ab
    }

    literal match {
      case v: Boolean => builder.setBoolean(v)
      case v: Byte => builder.setByte(v)
      case v: Short => builder.setShort(v)
      case v: Int => builder.setInteger(v)
      case v: Long => builder.setLong(v)
      case v: Float => builder.setFloat(v)
      case v: Double => builder.setDouble(v)
      case v: BigDecimal =>
        builder.setDecimal(decimalBuilder(v.precision, v.scale, v.toString))
      case v: JBigDecimal =>
        builder.setDecimal(decimalBuilder(v.precision, v.scale, v.toString))
      case v: String => builder.setString(v)
      case v: Char => builder.setString(v.toString)
      case v: Array[Char] => builder.setString(String.valueOf(v))
      case v: Array[Byte] => builder.setBinary(ByteString.copyFrom(v))
      case v: collection.mutable.WrappedArray[_] => toLiteralProtoBuilder(v.array)
      case v: LocalDate => builder.setDate(v.toEpochDay.toInt)
      case v: Decimal =>
        builder.setDecimal(decimalBuilder(Math.max(v.precision, v.scale), v.scale, v.toString))
      case v: Instant => builder.setTimestamp(DateTimeUtils.instantToMicros(v))
      case v: Timestamp => builder.setTimestamp(DateTimeUtils.fromJavaTimestamp(v))
      case v: LocalDateTime => builder.setTimestampNtz(DateTimeUtils.localDateTimeToMicros(v))
      case v: Date => builder.setDate(DateTimeUtils.fromJavaDate(v))
      case v: Duration => builder.setDayTimeInterval(IntervalUtils.durationToMicros(v))
      case v: Period => builder.setYearMonthInterval(IntervalUtils.periodToMonths(v))
      case v: Array[_] => builder.setArray(arrayBuilder(v))
      case v: CalendarInterval =>
        builder.setCalendarInterval(calendarIntervalBuilder(v.months, v.days, v.microseconds))
      case null => builder.setNull(ProtoDataTypes.NullType)
      case _ => throw new UnsupportedOperationException(s"literal $literal not supported (yet).")
    }
  }

  /**
   * Transforms literal value to the `proto.Expression.Literal`.
   *
   * @return
   *   proto.Expression.Literal
   */
  def toLiteralProto(literal: Any): proto.Expression.Literal =
    toLiteralProtoBuilder(literal).build()

  private def toDataType(clz: Class[_]): DataType = clz match {
    // primitive types
    case JShort.TYPE => ShortType
    case JInteger.TYPE => IntegerType
    case JLong.TYPE => LongType
    case JDouble.TYPE => DoubleType
    case JByte.TYPE => ByteType
    case JFloat.TYPE => FloatType
    case JBoolean.TYPE => BooleanType
    case JChar.TYPE => StringType

    // java classes
    case _ if clz == classOf[LocalDate] || clz == classOf[Date] => DateType
    case _ if clz == classOf[Instant] || clz == classOf[Timestamp] => TimestampType
    case _ if clz == classOf[LocalDateTime] => TimestampNTZType
    case _ if clz == classOf[Duration] => DayTimeIntervalType.DEFAULT
    case _ if clz == classOf[Period] => YearMonthIntervalType.DEFAULT
    case _ if clz == classOf[JBigDecimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[Array[Byte]] => BinaryType
    case _ if clz == classOf[Array[Char]] => StringType
    case _ if clz == classOf[JShort] => ShortType
    case _ if clz == classOf[JInteger] => IntegerType
    case _ if clz == classOf[JLong] => LongType
    case _ if clz == classOf[JDouble] => DoubleType
    case _ if clz == classOf[JByte] => ByteType
    case _ if clz == classOf[JFloat] => FloatType
    case _ if clz == classOf[JBoolean] => BooleanType

    // other scala classes
    case _ if clz == classOf[String] => StringType
    case _ if clz == classOf[BigInt] || clz == classOf[BigDecimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[CalendarInterval] => CalendarIntervalType
    case _ if clz.isArray => ArrayType(toDataType(clz.getComponentType))
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported component type $clz in arrays.")
  }

  def toCatalystValue(literal: proto.Expression.Literal): Any = {
    literal.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.NULL => null

      case proto.Expression.Literal.LiteralTypeCase.BINARY => literal.getBinary.toByteArray

      case proto.Expression.Literal.LiteralTypeCase.BOOLEAN => literal.getBoolean

      case proto.Expression.Literal.LiteralTypeCase.BYTE => literal.getByte.toByte

      case proto.Expression.Literal.LiteralTypeCase.SHORT => literal.getShort.toShort

      case proto.Expression.Literal.LiteralTypeCase.INTEGER => literal.getInteger

      case proto.Expression.Literal.LiteralTypeCase.LONG => literal.getLong

      case proto.Expression.Literal.LiteralTypeCase.FLOAT => literal.getFloat

      case proto.Expression.Literal.LiteralTypeCase.DOUBLE => literal.getDouble

      case proto.Expression.Literal.LiteralTypeCase.DECIMAL =>
        Decimal(literal.getDecimal.getValue)

      case proto.Expression.Literal.LiteralTypeCase.STRING => literal.getString

      case proto.Expression.Literal.LiteralTypeCase.DATE =>
        DateTimeUtils.toJavaDate(literal.getDate)

      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP =>
        DateTimeUtils.toJavaTimestamp(literal.getTimestamp)

      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ =>
        DateTimeUtils.microsToLocalDateTime(literal.getTimestampNtz)

      case proto.Expression.Literal.LiteralTypeCase.CALENDAR_INTERVAL =>
        new CalendarInterval(
          literal.getCalendarInterval.getMonths,
          literal.getCalendarInterval.getDays,
          literal.getCalendarInterval.getMicroseconds)

      case proto.Expression.Literal.LiteralTypeCase.YEAR_MONTH_INTERVAL =>
        IntervalUtils.monthsToPeriod(literal.getYearMonthInterval)

      case proto.Expression.Literal.LiteralTypeCase.DAY_TIME_INTERVAL =>
        IntervalUtils.microsToDuration(literal.getDayTimeInterval)

      case proto.Expression.Literal.LiteralTypeCase.ARRAY =>
        toCatalystArray(literal.getArray)

      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported Literal Type: ${other.getNumber} (${other.name})")
    }
  }

  def toCatalystArray(array: proto.Expression.Literal.Array): Array[_] = {
    def makeArrayData[T](converter: proto.Expression.Literal => T)(implicit
        tag: ClassTag[T]): Array[T] = {
      val builder = mutable.ArrayBuilder.make[T]
      val elementList = array.getElementsList
      builder.sizeHint(elementList.size())
      val iter = elementList.iterator()
      while (iter.hasNext) {
        builder += converter(iter.next())
      }
      builder.result()
    }

    val elementType = array.getElementType
    if (elementType.hasShort) {
      makeArrayData(v => v.getShort.toShort)
    } else if (elementType.hasInteger) {
      makeArrayData(v => v.getInteger)
    } else if (elementType.hasLong) {
      makeArrayData(v => v.getLong)
    } else if (elementType.hasDouble) {
      makeArrayData(v => v.getDouble)
    } else if (elementType.hasByte) {
      makeArrayData(v => v.getByte.toByte)
    } else if (elementType.hasFloat) {
      makeArrayData(v => v.getFloat)
    } else if (elementType.hasBoolean) {
      makeArrayData(v => v.getBoolean)
    } else if (elementType.hasString) {
      makeArrayData(v => v.getString)
    } else if (elementType.hasBinary) {
      makeArrayData(v => v.getBinary.toByteArray)
    } else if (elementType.hasDate) {
      makeArrayData(v => DateTimeUtils.toJavaDate(v.getDate))
    } else if (elementType.hasTimestamp) {
      makeArrayData(v => DateTimeUtils.toJavaTimestamp(v.getTimestamp))
    } else if (elementType.hasTimestampNtz) {
      makeArrayData(v => DateTimeUtils.microsToLocalDateTime(v.getTimestampNtz))
    } else if (elementType.hasDayTimeInterval) {
      makeArrayData(v => IntervalUtils.microsToDuration(v.getDayTimeInterval))
    } else if (elementType.hasYearMonthInterval) {
      makeArrayData(v => IntervalUtils.monthsToPeriod(v.getYearMonthInterval))
    } else if (elementType.hasDecimal) {
      makeArrayData(v => Decimal(v.getDecimal.getValue))
    } else if (elementType.hasCalendarInterval) {
      makeArrayData(v => {
        val interval = v.getCalendarInterval
        new CalendarInterval(interval.getMonths, interval.getDays, interval.getMicroseconds)
      })
    } else if (elementType.hasArray) {
      makeArrayData(v => toCatalystArray(v.getArray))
    } else {
      throw new UnsupportedOperationException(s"Unsupported Literal Type: $elementType)")
    }
  }
}
