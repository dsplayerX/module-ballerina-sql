/*
 *  Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package io.ballerina.stdlib.sql.parameterprocessor;

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.XmlUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BXml;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;
import io.ballerina.stdlib.sql.utils.Utils;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;

/**
 * This class implements methods required convert SQL types into ballerina types and
 * other methods that process the parameters of the result.
 *
 * @since 0.5.6
 */
public class DefaultResultParameterProcessor extends AbstractResultParameterProcessor {
    private static final Object lock = new Object();
    private static volatile DefaultResultParameterProcessor instance;

    private static final ArrayType stringArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_STRING);
    private static final ArrayType booleanArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_BOOLEAN);
    private static final ArrayType intArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_INT);
    private static final ArrayType floatArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_FLOAT);
    private static final ArrayType decimalArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_DECIMAL);
    private static final ArrayType mapArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_MAP);
    private static final ArrayType byteArrayType = TypeCreator.createArrayType(
        TypeCreator.createArrayType(PredefinedTypes.TYPE_BYTE));
    private static final RecordType civilRecordType = TypeCreator.createRecordType(
        io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD,
        io.ballerina.stdlib.time.util.ModuleUtils.getModule(), 0, true, 0);
    private static final ArrayType civilArrayType = TypeCreator.createArrayType(civilRecordType);
    private static final RecordType timeRecordType = TypeCreator.createRecordType(
        io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD,
        io.ballerina.stdlib.time.util.ModuleUtils.getModule(), 0, true, 0);
    private static final ArrayType timeArrayType = TypeCreator.createArrayType(timeRecordType);
    private static final RecordType dateRecordType = TypeCreator.createRecordType(
        io.ballerina.stdlib.time.util.Constants.DATE_RECORD,
        io.ballerina.stdlib.time.util.ModuleUtils.getModule(), 0, true, 0);
    private static final ArrayType dateArrayType = TypeCreator.createArrayType(dateRecordType);

    public DefaultResultParameterProcessor() {
    }

    public static DefaultResultParameterProcessor getInstance() {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new DefaultResultParameterProcessor();
                }
            }
        }
        return instance;
    }

    protected BArray createAndPopulateBBRefValueArray(Object firstNonNullElement, Object[] dataArray,
                                                      Type type, Array array) throws ApplicationError, SQLException {
        BArray refValueArray = null;
        int length = dataArray.length;
        if (firstNonNullElement instanceof String) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_STRING);
            for (int i = 0; i < length; i++) {
                if (dataArray[i] == null) {
                    refValueArray.append(null);
                } else {
                    refValueArray.append(fromString(String.valueOf(dataArray[i])));
                }
            }
            return refValueArray;
        } else if (firstNonNullElement instanceof Boolean) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_BOOLEAN);
        } else if (firstNonNullElement instanceof Short) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_INT);
        } else if (firstNonNullElement instanceof Integer) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_INT);
        } else if (firstNonNullElement instanceof Long) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_INT);
        } else if (firstNonNullElement instanceof Float) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_FLOAT);
        } else if (firstNonNullElement instanceof Double) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_FLOAT);
        } else if (firstNonNullElement instanceof BigDecimal) {
            refValueArray = createEmptyBBRefValueArray(PredefinedTypes.TYPE_DECIMAL);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i,
                        dataArray[i] != null ? ValueCreator.createDecimalValue((BigDecimal) dataArray[i]) : null);
            }
            return refValueArray;
        } else if (firstNonNullElement instanceof java.util.Date) {
            if (firstNonNullElement instanceof Date) {
                refValueArray = createEmptyBBRefValueArray(dateRecordType);
                for (int i = 0; i < length; i++) {                    
                    if (dataArray[i] == null) {
                        refValueArray.add(i, dataArray[i]);
                    } else {
                        BMap<BString, Object> dateMap = Utils.createDateRecord((Date) dataArray[i]);
                        refValueArray.add(i, dateMap);
                    }
                }
                return refValueArray;
            } else if (firstNonNullElement instanceof Time) {
                refValueArray = createEmptyBBRefValueArray(timeRecordType);
                for (int i = 0; i < length; i++) {                    
                    if (dataArray[i] == null) {
                        refValueArray.add(i, dataArray[i]);
                    } else {
                        BMap<BString, Object> timeMap = Utils.createTimeRecord((Time) dataArray[i]);
                        refValueArray.add(i, timeMap);
                    }
                }
                return refValueArray;
            } else if (firstNonNullElement instanceof Timestamp) {
                refValueArray = createEmptyBBRefValueArray(civilRecordType);
                for (int i = 0; i < length; i++) {
                    if (dataArray[i] == null) {
                        refValueArray.add(i, dataArray[i]);
                    } else {
                        BMap<BString, Object> civilMap = Utils.createTimestampRecord((Timestamp) dataArray[i]);
                        refValueArray.add(i, civilMap);
                    }
                }
                return refValueArray;
            } else {
                throw new ApplicationError("Error while retrieving Date Array");
            } 
        } else if (firstNonNullElement instanceof java.time.OffsetTime) {
            refValueArray = createEmptyBBRefValueArray(timeRecordType);
            for (int i = 0; i < length; i++) {                
                if (dataArray[i] == null) {
                    refValueArray.add(i, dataArray[i]);
                } else {
                    BMap<BString, Object> timeMap = 
                        Utils.createTimeWithTimezoneRecord((java.time.OffsetTime) dataArray[i]);
                    refValueArray.add(i, timeMap);
                }
            }
            return refValueArray;
        } else if (firstNonNullElement instanceof java.time.OffsetDateTime) {
            refValueArray = createEmptyBBRefValueArray(civilRecordType);
            for (int i = 0; i < length; i++) {                
                if (dataArray[i] == null) {
                    refValueArray.add(i, dataArray[i]);
                } else {
                    BMap<BString, Object> civilMap = 
                        Utils.createTimestampWithTimezoneRecord((java.time.OffsetDateTime) dataArray[i]);
                    refValueArray.add(i, civilMap);
                }
            }
            return refValueArray;
        } else if (firstNonNullElement instanceof byte[]) {            
            refValueArray = createEmptyBBRefValueArray(TypeCreator.createArrayType(PredefinedTypes.TYPE_BYTE));
            for (int i = 0; i < dataArray.length; i++) {                
                if (dataArray[i] == null) {
                    refValueArray.add(i, dataArray[i]);
                } else {
                    refValueArray.add(i, ValueCreator.createArrayValue((byte[]) dataArray[i]));
                }
            }            
            return refValueArray;
        } else if (firstNonNullElement == null) {
            refValueArray = createEmptyBBRefValueArray(type);
            for (int i = 0; i < length; i++) {
                refValueArray.add(i, firstNonNullElement);
            }
            return refValueArray;
        } else {
            return createAndPopulateCustomBBRefValueArray(firstNonNullElement, type, array);
        }
        for (int i = 0; i < length; i++) {
            refValueArray.add(i, dataArray[i]);
        }
        return refValueArray;
    }

    public BArray createEmptyBBRefValueArray(Type type) {
        List<Type> memberTypes = new ArrayList<>(2);
        memberTypes.add(type);
        memberTypes.add(PredefinedTypes.TYPE_NULL);
        UnionType unionType = TypeCreator.createUnionType(memberTypes);
        return ValueCreator.createArrayValue(TypeCreator.createArrayType(unionType));
    }

    @Override
    protected BArray createAndPopulateCustomBBRefValueArray(Object firstNonNullElement, Type type, Array array)
            throws ApplicationError, SQLException {
        return null;
    }

    protected BArray createAndPopulatePrimitiveValueArray(Object firstNonNullElement, Object[] dataArray,
                                                          Type type, Array array)
            throws ApplicationError, SQLException {
        int length = dataArray.length;
        String elementType = firstNonNullElement.getClass().getCanonicalName();
        switch (elementType) {
            case Constants.Classes.STRING:
                BArray stringDataArray = ValueCreator.createArrayValue(stringArrayType);
                for (int i = 0; i < length; i++) {
                    stringDataArray.add(i, fromString((String) dataArray[i]));
                }
                return stringDataArray;
            case Constants.Classes.BOOLEAN:
                BArray boolDataArray = ValueCreator.createArrayValue(booleanArrayType);
                for (int i = 0; i < length; i++) {
                    boolDataArray.add(i, ((Boolean) dataArray[i]).booleanValue());
                }
                return boolDataArray;
            case Constants.Classes.SHORT:
                BArray shortDataArray = ValueCreator.createArrayValue(intArrayType);
                for (int i = 0; i < length; i++) {
                    shortDataArray.add(i, ((Short) dataArray[i]).intValue());
                }
                return shortDataArray;
            case Constants.Classes.INTEGER:
                BArray intDataArray = ValueCreator.createArrayValue(intArrayType);
                for (int i = 0; i < length; i++) {
                    intDataArray.add(i, ((Integer) dataArray[i]).intValue());
                }
                return intDataArray;
            case Constants.Classes.LONG:
                BArray longDataArray = ValueCreator.createArrayValue(intArrayType);
                for (int i = 0; i < length; i++) {
                    longDataArray.add(i, ((Long) dataArray[i]).longValue());
                }
                return longDataArray;
            case Constants.Classes.FLOAT:
                BArray floatDataArray = ValueCreator.createArrayValue(floatArrayType);
                for (int i = 0; i < length; i++) {
                    floatDataArray.add(i, ((Float) dataArray[i]).floatValue());
                }
                return floatDataArray;
            case Constants.Classes.DOUBLE:
                BArray doubleDataArray = ValueCreator.createArrayValue(floatArrayType);
                for (int i = 0; i < dataArray.length; i++) {
                    doubleDataArray.add(i, ((Double) dataArray[i]).doubleValue());
                }
                return doubleDataArray;
            case Constants.Classes.BIG_DECIMAL:
                BArray decimalDataArray = ValueCreator.createArrayValue(decimalArrayType);
                for (int i = 0; i < dataArray.length; i++) {
                    decimalDataArray.add(i, ValueCreator.createDecimalValue((BigDecimal) dataArray[i]));
                }
                return decimalDataArray;
            case Constants.Classes.BYTE:
                BArray byteDataArray = ValueCreator.createArrayValue(byteArrayType);
                for (int i = 0; i < dataArray.length; i++) {
                    byteDataArray.add(i, ValueCreator.createArrayValue((byte[]) dataArray[i]));
                }
                return byteDataArray;
            case Constants.Classes.DATE:
                BArray mapDataArray;
                mapDataArray = ValueCreator.createArrayValue(dateArrayType);
                for (int i = 0; i < dataArray.length; i++) {
                    BMap<BString, Object> dateMap = Utils.createDateRecord((Date) dataArray[i]);
                    mapDataArray.add(i, dateMap);
                }
                return mapDataArray;
            case Constants.Classes.TIMESTAMP:
                mapDataArray = ValueCreator.createArrayValue(civilArrayType);
                for (int i = 0; i < dataArray.length; i++) {
                    BMap<BString, Object> civilMap = Utils.createTimestampRecord((Timestamp) dataArray[i]);
                    mapDataArray.add(i, civilMap);
                }
                return mapDataArray;
            case Constants.Classes.TIME:
                mapDataArray = ValueCreator.createArrayValue(timeArrayType);
                for (int i = 0; i < dataArray.length; i++) {
                    BMap<BString, Object> timeMap = Utils.createTimeRecord((Time) dataArray[i]);
                    mapDataArray.add(i, timeMap);
                }
                return mapDataArray;
            case Constants.Classes.OFFSET_TIME:
                BArray mapTimeArray = ValueCreator.createArrayValue(timeArrayType);
                for (int i = 0; i < dataArray.length; i++) {
                    BMap<BString, Object> civilMap =
                            Utils.createTimeWithTimezoneRecord((java.time.OffsetTime) dataArray[i]);
                    mapTimeArray.add(i, civilMap);
                }
                return mapTimeArray;
            case Constants.Classes.OFFSET_DATE_TIME:
                BArray mapDateTimeArray = ValueCreator.createArrayValue(civilArrayType);
                for (int i = 0; i < dataArray.length; i++) {
                    BMap<BString, Object> civilMap =
                            Utils.createTimestampWithTimezoneRecord((java.time.OffsetDateTime) dataArray[i]);
                    mapDateTimeArray.add(i, civilMap);
                }
                return mapDateTimeArray;
            default:
                return createAndPopulateCustomValueArray(firstNonNullElement, type, array);
        }
    }

    @Override
    protected BArray createAndPopulateCustomValueArray(Object firstNonNullElement, Type type, Array array)
         throws ApplicationError, SQLException {
        return null;
    }

    protected BMap<BString, Object> createUserDefinedType(Struct structValue, StructureType structType)
            throws ApplicationError {
        if (structValue == null) {
            return null;
        }
        Field[] internalStructFields = structType.getFields().values().toArray(new Field[0]);
        BMap<BString, Object> struct = ValueCreator.createMapValue(structType);
        try {
            Object[] dataArray = structValue.getAttributes();
            if (dataArray != null) {
                if (dataArray.length != internalStructFields.length) {
                    throw new ApplicationError("specified record and the returned SQL Struct field counts " +
                            "are different, and hence not compatible");
                }
                int index = 0;
                for (Field internalField : internalStructFields) {
                    int type = internalField.getFieldType().getTag();
                    BString fieldName = fromString(internalField.getFieldName());
                    Object value = dataArray[index];
                    switch (type) {
                        case TypeTags.INT_TAG:
                            if (value instanceof BigDecimal) {
                                struct.put(fieldName, ((BigDecimal) value).intValue());
                            } else {
                                struct.put(fieldName, value);
                            }
                            break;
                        case TypeTags.FLOAT_TAG:
                            if (value instanceof BigDecimal) {
                                struct.put(fieldName, ((BigDecimal) value).doubleValue());
                            } else {
                                struct.put(fieldName, value);
                            }
                            break;
                        case TypeTags.DECIMAL_TAG:
                            if (value instanceof BigDecimal) {
                                struct.put(fieldName, value);
                            } else {
                                struct.put(fieldName, ValueCreator.createDecimalValue((BigDecimal) value));
                            }
                            break;
                        case TypeTags.STRING_TAG:
                            struct.put(fieldName, value);
                            break;
                        case TypeTags.BOOLEAN_TAG:
                            struct.put(fieldName, ((int) value) == 1);
                            break;
                        case TypeTags.OBJECT_TYPE_TAG:
                        case TypeTags.RECORD_TYPE_TAG:
                            struct.put(fieldName,
                                    createUserDefinedType((Struct) value,
                                            (StructureType) internalField.getFieldType()));
                            break;
                        default:
                            createUserDefinedTypeSubtype(internalField, structType);
                    }
                    ++index;
                }
            }
        } catch (SQLException e) {
            throw new ApplicationError("Error while retrieving data to create " + structType.getName()
                    + " record. ", e);
        }
        return struct;
    }

    @Override
    protected void createUserDefinedTypeSubtype(Field internalField, StructureType structType)
            throws ApplicationError {
        throw new ApplicationError("Error while retrieving data for unsupported type " +
                internalField.getFieldType().getName() + " to create "
                + structType.getName() + " record.");
    }

    protected static boolean containsNullObject(Object[] objects) {
        for (Object object : objects) {
            if (object == null) {
                return true;
            }
        }
        return false;
    }

    protected static Object firstNonNullObject(Object[] objects) {
        for (Object object : objects) {
            if (object != null) {
                return object;
            }
        }
        return null;
    }

    @Override
    public BArray convertArray(Array array, int sqlType, Type type) throws SQLException, ApplicationError {
        if (array != null) {
            Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Array");
            Object[] dataArray = (Object[]) array.getArray();
            if (dataArray == null || dataArray.length == 0) {
                return null;
            }
            Object firstNonNullElement = firstNonNullObject(dataArray);
            boolean containsNull = containsNullObject(dataArray);

            if (containsNull) {
                // If there are some null elements, return a union-type element array
                return createAndPopulateBBRefValueArray(firstNonNullElement, dataArray, type, array);
            } else {
                // If there are no null elements, return a ballerina primitive-type array
                return createAndPopulatePrimitiveValueArray(firstNonNullElement, dataArray, type, array);
            }

        } else {
            return null;
        }
    }

    @Override
    public BString convertChar(String value, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL String");
        return fromString(value);
    }

    @Override
    public Object convertChar(
            String value, int sqlType, Type type, String sqlTypeName) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, sqlTypeName);
        return fromString(value);
    }

    @Override
    public Object convertByteArray(byte[] value, int sqlType, Type type, String sqlTypeName) throws ApplicationError {
        if (value != null) {
            return ValueCreator.createArrayValue(value);
        } else {
            return null;
        }
    }

    @Override
    public Object convertBinary(Object value, int sqlType, Type ballerinaType) throws ApplicationError {
        if (ballerinaType.getTag() == TypeTags.STRING_TAG) {
            return convertChar((String) value, sqlType, ballerinaType);
        } else {
            return convertByteArray(((String) value).getBytes(Charset.defaultCharset()), sqlType, ballerinaType,
                    JDBCType.valueOf(sqlType).getName()
            );
        }
    }

    @Override
    public Object convertInteger(long value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL long or integer");
        if (isNull) {
            return null;
        } else {
            if (type.getTag() == TypeTags.STRING_TAG) {
                return fromString(String.valueOf(value));
            }
            return value;
        }
    }

    @Override
    public Object convertDouble(double value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL double or float");
        if (isNull) {
            return null;
        } else {
            if (type.getTag() == TypeTags.STRING_TAG) {
                return fromString(String.valueOf(value));
            }
            return value;
        }
    }

    @Override
    public Object convertDecimal(BigDecimal value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL decimal or real");
        if (isNull) {
            return null;
        } else {
            if (type.getTag() == TypeTags.STRING_TAG) {
                return fromString(String.valueOf(value));
            }
            return ValueCreator.createDecimalValue(value);
        }
    }

    @Override
    public Object convertBlob(Blob value, int sqlType, Type type) throws ApplicationError, SQLException {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Blob");
        if (value != null) {
            return ValueCreator.createArrayValue(value.getBytes(1L, (int) value.length()));
        } else {
            return null;
        }
    }

    @Override
    public Object convertDate(java.util.Date date, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (date != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(date.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    if (type.getName().equals(io.ballerina.stdlib.time.util.Constants.DATE_RECORD)) {
                        if (date instanceof Date) {
                            return Utils.createDateRecord((Date) date);
                        } else {
                            return fromString(date.toString());
                        }
                    } else {
                        throw new ApplicationError("Unsupported Ballerina type:" +
                            type.getName() + " for SQL Date data type.");
                    }
                case TypeTags.INT_TAG:
                    return date.getTime();
            }
        }
        return null;
    }

    @Override
    public Object convertTime(java.util.Date time, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (time != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(time.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    if (type.getName().equals(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD)) {
                        if (time instanceof Time) {
                            return Utils.createTimeRecord((Time) time);
                        } else {
                            return fromString(time.toString());
                        }
                    } else {
                        throw new ApplicationError("Unsupported Ballerina type:" +
                            type.getName() + " for SQL Time data type.");
                    }
                case TypeTags.INT_TAG:
                    return time.getTime();
            }
        }
        return null;
    }

    @Override
    public Object convertTimeWithTimezone(java.time.OffsetTime offsetTime, int sqlType, Type type)
            throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (offsetTime != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(offsetTime.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    if (type.getName().equals(io.ballerina.stdlib.time.util.Constants.TIME_OF_DAY_RECORD)) {
                        return Utils.createTimeWithTimezoneRecord(offsetTime);
                    } else {
                        throw new ApplicationError("Unsupported Ballerina type:" +
                            type.getName() + " for SQL Time with timezone data type.");
                    }
                case TypeTags.INT_TAG:
                    return Time.valueOf(offsetTime.toLocalTime()).getTime();
            }
        }
        return null;
    }

    @Override
    public Object convertTimeStamp(java.util.Date timestamp, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (timestamp != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(timestamp.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                if (type.getName().equalsIgnoreCase(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD)
                        && timestamp instanceof Timestamp) {
                    return Utils.createTimestampRecord((Timestamp) timestamp);
                } else {
                    throw new ApplicationError("Unsupported Ballerina type:" +
                        type.getName() + " for SQL Timestamp data type.");
                }
                case TypeTags.INT_TAG:
                    return timestamp.getTime();
                case TypeTags.INTERSECTION_TAG:
                    return Utils.createTimeStruct(timestamp.getTime());
            }
        }
        return null;
    }

    @Override
    public Object convertTimestampWithTimezone(java.time.OffsetDateTime offsetDateTime, int sqlType, Type type)
            throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Date/Time");
        if (offsetDateTime != null) {
            switch (type.getTag()) {
                case TypeTags.STRING_TAG:
                    return fromString(offsetDateTime.toString());
                case TypeTags.OBJECT_TYPE_TAG:
                case TypeTags.RECORD_TYPE_TAG:
                    if (type.getName().equalsIgnoreCase(io.ballerina.stdlib.time.util.Constants.CIVIL_RECORD)) {
                        return Utils.createTimestampWithTimezoneRecord(offsetDateTime);
                    } else {
                        throw new ApplicationError("Unsupported Ballerina type:" +
                            type.getName() + " for SQL Timestamp with timezone data type.");
                    }
                case TypeTags.INT_TAG:
                    return Timestamp.valueOf(offsetDateTime.toLocalDateTime()).getTime();
                case TypeTags.INTERSECTION_TAG:
                    return Utils.createTimeStruct(Timestamp.valueOf(
                            offsetDateTime.atZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime()).getTime());
            }
        }
        return null;
    }

    @Override
    public Object convertBoolean(boolean value, int sqlType, Type type, boolean isNull) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Boolean");
        if (!isNull) {
            switch (type.getTag()) {
                case TypeTags.BOOLEAN_TAG:
                    return value;
                case TypeTags.INT_TAG:
                    if (value) {
                        return 1L;
                    } else {
                        return 0L;
                    }
                case TypeTags.STRING_TAG:
                    return fromString(String.valueOf(value));
            }
        }
        return null;
    }

    @Override
    public Object convertStruct(Struct value, int sqlType, Type type) throws ApplicationError {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL Struct");
        if (value != null) {
            if (type instanceof RecordType) {
                return createUserDefinedType(value, (RecordType) type);
            } else {
                throw new ApplicationError("The ballerina type that can be used for SQL struct should be record type," +
                        " but found " + type.getName() + " .");
            }
        } else {
            return null;
        }
    }

    @Override
    public Object convertXml(SQLXML value, int sqlType, Type type) throws ApplicationError, SQLException {
        Utils.validatedInvalidFieldAssignment(sqlType, type, "SQL XML");
        if (value != null) {
            if (type instanceof BXml) {
                return XmlUtils.parse(value.getBinaryStream());
            } else {
                throw new ApplicationError("The ballerina type that can be used for SQL struct should be record type," +
                        " but found " + type.getName() + " .");
            }
        } else {
            return null;
        }
    }

    private Object processCharacterTypes(CallableStatement statement, int paramIndex)
            throws SQLException {
        return statement.getString(paramIndex);
    }

    private Object processFloatAndReal(CallableStatement statement, int paramIndex)
            throws SQLException {
        return statement.getFloat(paramIndex);
    }

    private Object processNumericAndDecimal(CallableStatement statement, int paramIndex)
            throws SQLException {
        return statement.getBigDecimal(paramIndex);
    }

    private Object processBitAndBoolean(CallableStatement statement, int paramIndex)
            throws SQLException {
        return statement.getBoolean(paramIndex);
    }

    private Object processRefAndStruct(CallableStatement statement, int paramIndex)
            throws SQLException {
        return statement.getObject(paramIndex);
    }

    @Override
    public Object processChar(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processCharacterTypes(statement, paramIndex);
    }

    @Override
    public Object processVarchar(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processCharacterTypes(statement, paramIndex);
    }

    @Override
    public Object processLongVarchar(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processCharacterTypes(statement, paramIndex);
    }

    @Override
    public Object processNChar(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processCharacterTypes(statement, paramIndex);
    }

    @Override
    public Object processNVarchar(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processCharacterTypes(statement, paramIndex);
    }

    @Override
    public Object processLongNVarchar(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processCharacterTypes(statement, paramIndex);
    }

    @Override
    public Object processBinary(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processCharacterTypes(statement, paramIndex);
    }

    @Override

    public Object processVarBinary(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processCharacterTypes(statement, paramIndex);
    }

    @Override
    public Object processLongVarBinary(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processCharacterTypes(statement, paramIndex);
    }

    @Override
    public Object processBlob(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getBlob(paramIndex);
    }

    @Override
    public Object processClob(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getClob(paramIndex);
    }

    @Override
    public Object processNClob(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getNClob(paramIndex);
    }

    @Override
    public Object processDate(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getDate(paramIndex);
    }

    @Override
    public Object processTime(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getTime(paramIndex);
    }

    @Override
    public Object processTimeWithTimeZone(CallableStatement statement, int paramIndex)
            throws SQLException {
        return statement.getObject(paramIndex, OffsetTime.class);
    }

    @Override
    public Object processTimestamp(CallableStatement statement, int paramIndex)
            throws SQLException {
        return statement.getTimestamp(paramIndex);
    }

    @Override
    public Object processTimestampWithTimeZone(CallableStatement statement, int paramIndex)
            throws SQLException {
        return statement.getObject(paramIndex, OffsetDateTime.class);
    }

    @Override
    public Object processArray(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getArray(paramIndex);
    }

    @Override
    public Object processRowID(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getRowId(paramIndex);
    }

    @Override
    public Object processTinyInt(CallableStatement statement, int paramIndex)
            throws SQLException {
        return statement.getInt(paramIndex);
    }

    @Override
    public Object processSmallInt(CallableStatement statement, int paramIndex)
            throws SQLException {
        return Integer.valueOf(statement.getShort(paramIndex));
    }

    @Override
    public Object processInteger(CallableStatement statement, int paramIndex)
            throws SQLException {
        return Long.valueOf(statement.getInt(paramIndex));
    }

    @Override
    public Object processBigInt(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getLong(paramIndex);
    }

    @Override
    public Object processReal(CallableStatement statement, int paramIndex) throws SQLException {
        return processFloatAndReal(statement, paramIndex);
    }

    @Override
    public Object processFloat(CallableStatement statement, int paramIndex)
            throws SQLException {
        return processFloatAndReal(statement, paramIndex);
    }

    @Override
    public Object processDouble(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getDouble(paramIndex);
    }

    @Override
    public Object processNumeric(CallableStatement statement, int paramIndex) throws SQLException {
        return processNumericAndDecimal(statement, paramIndex);
    }

    @Override
    public Object processDecimal(CallableStatement statement, int paramIndex) throws SQLException {
        return processNumericAndDecimal(statement, paramIndex);
    }

    @Override
    public Object processBit(CallableStatement statement, int paramIndex) throws SQLException {
        return processBitAndBoolean(statement, paramIndex);
    }

    @Override
    public Object processBoolean(CallableStatement statement, int paramIndex) throws SQLException {
        return processBitAndBoolean(statement, paramIndex);
    }

    @Override
    public Object processRef(CallableStatement statement, int paramIndex) throws SQLException {
        return processRefAndStruct(statement, paramIndex);
    }

    @Override
    public Object processStruct(CallableStatement statement, int paramIndex) throws SQLException {
        return processRefAndStruct(statement, paramIndex);
    }

    @Override
    public Object processXML(CallableStatement statement, int paramIndex) throws SQLException {
        return statement.getSQLXML(paramIndex);
    }

    @Override
    public Object processCustomOutParameters(CallableStatement statement, int paramIndex, int sqlType)
            throws ApplicationError {
        throw new ApplicationError("Unsupported SQL type '" + sqlType + "' when reading Procedure call " +
                "Out parameter of index '" + paramIndex + "'.");
    }

    @Override
    public Object processCustomTypeFromResultSet(ResultSet resultSet, int columnIndex,
                                                  ColumnDefinition columnDefinition) throws ApplicationError {
        throw new ApplicationError("Unsupported SQL type " + columnDefinition.getSqlName());
    }

    @Override
    public Object convertCustomOutParameter(Object value, String outParamObjectName, int sqlType, Type ballerinaType) {
        return ErrorGenerator.getSQLApplicationError("Unsupported SQL type " + sqlType);
    }

    @Override
    public Object convertCustomInOutParameter(Object value, Object inParamValue, int sqlType, Type ballerinaType) {
        return ErrorGenerator.getSQLApplicationError("Unsupported SQL type " + sqlType);
    }

    public BObject getBalStreamResultIterator() {
        return null;
    }

}