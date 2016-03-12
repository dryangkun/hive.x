package com.github.dryangkun.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

public class HxContext {

    public static final Log LOG = LogFactory.getLog(HxContext.class);
    
    protected final String[] args;
    protected final HxAbstractScriptExtension extension;
    protected final HxOutput output;

    public HxContext(String[] args, HxAbstractScriptExtension extension) {
        this.args = args;
        this.extension = extension;
        this.output = createOutput(extension.outputObjInspector);
    }

    protected HxOutput createOutput(ObjectInspector oi) {
        return HxOutput.create(oi);
    }

    public String[] getArgs() {
        return args;
    }

    public Configuration getConf() {
        return extension.conf;
    }

    public Log getLogger() {
        return LOG;
    }

    public LongWritable getCounter(String name) {
        if (extension.statsMap.containsKey(name)) {
            return extension.statsMap.get(name);
        } else {
            LongWritable value = new LongWritable();
            extension.statsMap.put(name, value);
            return value;
        }
    }

    public HxOutput getOutput() {
        return output;
    }

    public void forward() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("forward output - " + Arrays.asList(output.getBuffer()));
        }
        LOG.info("forward output - " + Arrays.asList(output.getBuffer()));
        extension.operator.forwardForExtension(
                output.getBuffer(), extension.outputObjInspector);
    }

    public static Boolean toBoolean(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getBoolean(data, (PrimitiveObjectInspector) oi);
    }

    public static Byte toByte(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getByte(data, (PrimitiveObjectInspector) oi);
    }

    public static Short toShort(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getShort(data, (PrimitiveObjectInspector) oi);
    }

    public static Integer toInt(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getInt(data, (PrimitiveObjectInspector) oi);
    }

    public static Long toLong(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getLong(data, (PrimitiveObjectInspector) oi);
    }

    public static Float toFloat(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getFloat(data, (PrimitiveObjectInspector) oi);
    }

    public static Double toDouble(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getDouble(data, (PrimitiveObjectInspector) oi);
    }

    public static String toString(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getString(data, (PrimitiveObjectInspector) oi);
    }

    public static HiveChar toChar(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getHiveChar(data, (PrimitiveObjectInspector) oi);
    }

    public static HiveVarchar toVarchar(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getHiveVarchar(data, (PrimitiveObjectInspector) oi);
    }

    public static BytesWritable toBinary(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getBinary(data, (PrimitiveObjectInspector) oi);
    }

    public static HiveDecimal toDecimal(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getHiveDecimal(data, (PrimitiveObjectInspector) oi);
    }

    public static Date toDate(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getDate(data, (PrimitiveObjectInspector) oi);
    }

    public static HiveIntervalYearMonth toIntervalYearMonth(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(data, (PrimitiveObjectInspector) oi);
    }

    public static Timestamp toTimestamp(Object data, ObjectInspector oi) {
        return PrimitiveObjectInspectorUtils.getTimestamp(data, (PrimitiveObjectInspector) oi);
    }

    public static Object toPrimitiveObject(Object data, PrimitiveObjectInspector oi) {
        if (data == null) {
            return null;
        }

        Object result = null;
        switch (oi.getPrimitiveCategory()) {
            case VOID:
                break;
            case BOOLEAN:
                result = ((BooleanObjectInspector) oi).get(data);
                break;
            case BYTE:
                result = ((ByteObjectInspector) oi).get(data);
                break;
            case SHORT:
                result = ((ShortObjectInspector) oi).get(data);
                break;
            case INT:
                result = ((IntObjectInspector) oi).get(data);
                break;
            case LONG:
                result = ((LongObjectInspector) oi).get(data);
                break;
            case FLOAT:
                result = ((FloatObjectInspector) oi).get(data);
                break;
            case DOUBLE:
                result = ((DoubleObjectInspector) oi).get(data);
                break;
            case STRING:
                result = ((StringObjectInspector) oi).getPrimitiveJavaObject(data);
                break;
            case CHAR:
                result = ((HiveCharObjectInspector) oi).getPrimitiveJavaObject(data);
                break;
            case VARCHAR:
                result = ((HiveVarcharObjectInspector)oi).getPrimitiveJavaObject(data);
                break;
            case BINARY:
                result = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(data);
                break;
            case DECIMAL:
                result = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(data);
                break;
            case DATE:
                result = ((DateObjectInspector) oi).getPrimitiveWritableObject(data).get();
                break;
            case INTERVAL_YEAR_MONTH:
                result = ((HiveIntervalYearMonthObjectInspector) oi).getPrimitiveJavaObject(data);
                break;
            case INTERVAL_DAY_TIME:
                result = ((HiveIntervalDayTimeObjectInspector) oi).getPrimitiveJavaObject(data);
                break;
            case TIMESTAMP:
                result = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(data).getTimestamp();
                break;
        }
        return result;
    }

    public static Object toObject(Object data, ObjectInspector oi) {
        if (data == null) {
            return null;
        }

        Object result = null;
        switch (oi.getCategory()) {
            case PRIMITIVE:
                result = toPrimitiveObject(data, (PrimitiveObjectInspector) oi);
                break;
            case LIST:
                ListObjectInspector listOI = (ListObjectInspector) oi;
                int listLength = listOI.getListLength(data);

                ObjectInspector listElementOI = listOI.getListElementObjectInspector();
                List<Object> listResult = new ArrayList<>(listLength);
                for (int i = 0; i < listLength; i++) {
                    Object listElement = listOI.getListElement(data, i);
                    listResult.add(toObject(listElement, listElementOI));
                }
                result = listResult;
                break;
            case MAP:
                MapObjectInspector mapOI = (MapObjectInspector) oi;
                ObjectInspector mapKeyOI = mapOI.getMapKeyObjectInspector();
                ObjectInspector mapValueOI = mapOI.getMapValueObjectInspector();
                Map<?, ?> map = mapOI.getMap(data);

                Map<Object, Object> mapResult = new HashMap<>(map.size());
                for (Map.Entry<Object, Object> kv : mapResult.entrySet()) {
                    Object k = toObject(kv.getKey(), mapKeyOI);
                    Object v = toObject(kv.getValue(), mapValueOI);
                    mapResult.put(k, v);
                }
                result = mapResult;
                break;
            case UNION:
                UnionObjectInspector unionOI = (UnionObjectInspector) oi;
                ObjectInspector unionFieldOI = unionOI.getObjectInspectors()
                        .get(unionOI.getTag(data));
                result = toObject(unionOI.getField(data), unionFieldOI);
                break;
            case STRUCT:
                StructObjectInspector structOI = (StructObjectInspector) oi;
                List<? extends StructField> structFields = structOI.getAllStructFieldRefs();

                List<Object> structResult = new ArrayList<>();
                for (StructField structField : structFields) {
                    Object v = structOI.getStructFieldData(data, structField);
                    structResult.add(toObject(v, structField.getFieldObjectInspector()));
                }
                result = structResult;
                break;
        }
        return result;
    }

    public static HxLazyArray toLazyArray(Object data, ObjectInspector oi) {
        return new HxLazyArray((ListObjectInspector) oi, data);
    }

    public static HxLazyMap toLazyMap(Object data, ObjectInspector oi) {
        return new HxLazyMap((MapObjectInspector) oi, data);
    }

    public static HxLazyUnion toLazyUnion(Object data, ObjectInspector oi) {
        return new HxLazyUnion((UnionObjectInspector) oi, data);
    }

    public static HxLazyStruct toLazyStruct(Object data, ObjectInspector oi) {
        return new HxLazyStruct((StructObjectInspector) oi, data);
    }

    public static HiveChar createChar(String data) {
        HiveChar hiveChar = new HiveChar();
        hiveChar.setValue(data);
        return hiveChar;
    }

    public static HiveChar createChar(String data, int length) {
        HiveChar hiveChar = new HiveChar();
        hiveChar.setValue(data, length);
        return hiveChar;
    }

    public static HiveVarchar createVarchar(String data) {
        HiveVarchar hiveChar = new HiveVarchar();
        hiveChar.setValue(data);
        return hiveChar;
    }

    public static HiveVarchar createVarchar(String data, int length) {
        HiveVarchar hiveChar = new HiveVarchar();
        hiveChar.setValue(data, length);
        return hiveChar;
    }

    public static HiveDecimal createDecimal(BigDecimal data) {
        return HiveDecimal.create(data);
    }

    public static HiveDecimal createDecimal(int data) {
        return HiveDecimal.create(BigDecimal.valueOf(data));
    }

    public static HiveDecimal createDecimal(long data) {
        return HiveDecimal.create(BigDecimal.valueOf(data));
    }

    public static HiveDecimal createDecimal(float data) {
        return HiveDecimal.create(BigDecimal.valueOf(data));
    }

    public static HiveDecimal createDecimal(double data) {
        return HiveDecimal.create(BigDecimal.valueOf(data));
    }

    public static Date createDate(long timestamp) {
        return new Date(timestamp);
    }

    public static HiveIntervalYearMonth createHiveIntervalYearMonth(int years, int months) {
        return new HiveIntervalYearMonth(years, months);
    }

    public static Timestamp createTimestamp(long data) {
        return new Timestamp(data);
    }
}
