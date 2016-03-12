package com.github.dryangkun.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

public class HxLazyStruct {

    private static final Log LOG = LogFactory.getLog(HxLazyStruct.class);

    private final StructObjectInspector oi;
    private final List<StructField> structFields;

    private Object data;

    @SuppressWarnings("unchecked")
    public HxLazyStruct(StructObjectInspector oi) {
        this.oi = oi;
        List<StructField> structFields = (List<StructField>) oi.getAllStructFieldRefs();
        if (!(structFields instanceof RandomAccess)) {
            structFields = new ArrayList<>(structFields);
        }
        this.structFields = structFields;
    }

    public HxLazyStruct(StructObjectInspector oi, Object data) {
        this(oi);
        this.data = data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Boolean getBoolean(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toBoolean(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public Byte getByte(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toByte(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public Short getShort(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toShort(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public Integer getInt(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toInt(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public Long getLong(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toLong(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public Float getFloat(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toFloat(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public Double getDouble(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toDouble(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public String getString(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toString(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public HiveChar getChar(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toChar(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public HiveVarchar getVarchar(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toVarchar(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public BytesWritable getBinary(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toBinary(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public HiveDecimal getDecimal(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toDecimal(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public Date getDate(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toDate(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public HiveIntervalYearMonth getIntervalYearMonth(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toIntervalYearMonth(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public Timestamp getTimestamp(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toTimestamp(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public Object getPrimitiveObject(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toPrimitiveObject(fieldObject, (PrimitiveObjectInspector) fieldOI);
        } else {
            return null;
        }
    }

    public Object getObject(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toObject(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public HxLazyArray getLazyArray(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toLazyArray(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public HxLazyMap getLazyMap(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toLazyMap(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public HxLazyUnion getLazyUnion(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toLazyUnion(fieldObject, fieldOI);
        } else {
            return null;
        }
    }

    public HxLazyStruct getLazyStruct(int index) {
        StructField structField = structFields.get(index);
        if (structField != null) {
            Object fieldObject = oi.getStructFieldData(data, structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();
            return HxContext.toLazyStruct(fieldObject, fieldOI);
        } else {
            return null;
        }
    }
}
