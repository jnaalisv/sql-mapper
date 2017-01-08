/*
 Copyright 2012, Brett Wooldridge

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.zaxxer.sansorm.internal;

import org.jnaalisv.sqlmapper.internal.TableSpecs;
import org.jnaalisv.sqlmapper.internal.TypeMapper;

import javax.persistence.*;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Introspected implements TableSpecs {
    private String tableName;

    private Map<String, FieldColumnInfo> columnToField = new LinkedHashMap<>();

    private String[] idColumnNames;
    private String[] columnNames;
    private String[] columnTableNames;

    Introspected(Class<?> clazz) throws IllegalAccessException, InstantiationException {

        Table tableAnnotation = clazz.getAnnotation(Table.class);
        if (tableAnnotation != null) {
            tableName = tableAnnotation.name();
        }
        ArrayList<FieldColumnInfo> idFcInfos = new ArrayList<FieldColumnInfo>();

        for (Field field : clazz.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) || Modifier.isFinal(modifiers) || Modifier.isTransient(modifiers)) {
                continue;
            }

            field.setAccessible(true);

            FieldColumnInfo fcInfo = new FieldColumnInfo(field);

            processColumnAnnotation(fcInfo);


            Id idAnnotation = field.getAnnotation(Id.class);
            Version versionAnnotation = field.getAnnotation(Version.class);

            if (versionAnnotation != null) {
                String versionColumnName = field.getName().toLowerCase();
            } else if (idAnnotation != null) {
                // Is it a problem that Class.getDeclaredFields() claims the fields are returned unordered?  We count on order.
                idFcInfos.add(fcInfo);
                GeneratedValue generatedAnnotation = field.getAnnotation(GeneratedValue.class);
                boolean isGeneratedId = (generatedAnnotation != null);
                if (isGeneratedId && idFcInfos.size() > 1) {
                    throw new IllegalStateException("Cannot have multiple @Id annotations and @GeneratedValue at the same time.");
                }
            }

            Enumerated enumAnnotation = field.getAnnotation(Enumerated.class);
            if (enumAnnotation != null) {
                fcInfo.setEnumConstants(enumAnnotation.value());
            }
        }

        readColumnInfo(idFcInfos);
    }

    public void set(Object target, String columnName, Object value) throws IllegalAccessException, IOException, SQLException {
        FieldColumnInfo fcInfo = columnToField.get(columnName);
        if (fcInfo == null) {
            throw new RuntimeException("Cannot find field mapped to column " + columnName + " on type " + target.getClass().getCanonicalName());
        }

        final Class<?> fieldType = fcInfo.fieldType;
        Class<?> columnType = value.getClass();
        Object columnValue = value;

        if (fcInfo.getConverter() != null) {
            columnValue = fcInfo.getConverter().convertToEntityAttribute(columnValue);
        } else if (fieldType != columnType) {
            // Fix-up column value for enums, integer as boolean, etc.
            if (fieldType == boolean.class && columnType == Integer.class) {
                columnValue = (((Integer) columnValue) != 0);
            } else if (columnType == BigDecimal.class) {
                if (fieldType == BigInteger.class) {
                    columnValue = ((BigDecimal) columnValue).toBigInteger();
                } else if (fieldType == Integer.class) {
                    columnValue = (int) ((BigDecimal) columnValue).longValue();
                } else if (fieldType == Long.class) {
                    columnValue = ((BigDecimal) columnValue).longValue();
                }
            } else if (fcInfo.enumConstants != null) {
                columnValue = fcInfo.enumConstants.get(columnValue);
            } else if (columnValue instanceof Clob) {
                columnValue = TypeMapper.readClob((Clob) columnValue);
            } else if (columnValue instanceof java.sql.Date && fieldType == LocalDate.class) {
                java.sql.Date date = (Date) columnValue;
                columnValue = date.toLocalDate();
            } else if (columnValue instanceof java.sql.Timestamp && fieldType == LocalDateTime.class) {
                java.sql.Timestamp timestamp = (Timestamp) columnValue;
                columnValue = timestamp.toLocalDateTime();
            }
        }

        fcInfo.field.set(target, columnValue);
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public String[] getColumnTableNames() {
        return columnTableNames;
    }

    public String[] getIdColumnNames() {
        return idColumnNames;
    }

    public String getTableName() {
        return tableName;
    }

    private void readColumnInfo(ArrayList<FieldColumnInfo> idFcInfos) {
        FieldColumnInfo[] idFieldColumnInfos = new FieldColumnInfo[idFcInfos.size()];
        idColumnNames = new String[idFcInfos.size()];
        int i = 0;
        int j = 0;
        for (FieldColumnInfo fcInfo : idFcInfos) {
            idColumnNames[i] = fcInfo.columnName;
            idFieldColumnInfos[i] = fcInfo;
            ++i;
        }

        columnNames = new String[columnToField.size()];
        columnTableNames = new String[columnNames.length];
        i = 0;
        j = 0;
        for (Entry<String, FieldColumnInfo> entry : columnToField.entrySet()) {
            columnNames[i] = entry.getKey();
            columnTableNames[i] = entry.getValue().columnTableName;
            if (!idFcInfos.contains(entry.getValue())) {
                ++j;
            }
            ++i;
        }
    }

    private void processColumnAnnotation(FieldColumnInfo fcInfo) {
        Field field = fcInfo.field;
        Column columnAnnotation = field.getAnnotation(Column.class);
        if (columnAnnotation != null) {
            fcInfo.columnName = columnAnnotation.name().toLowerCase();
            String columnTableName = columnAnnotation.table();
            if (columnTableName != null && columnTableName.length() > 0) {
                fcInfo.columnTableName = columnTableName.toLowerCase();
            }
        } else {
            fcInfo.columnName = field.getName().toLowerCase();
        }
    }

    private static class FieldColumnInfo {
        private String columnName;
        private String columnTableName;
        private Field field;
        private Class<?> fieldType;
        private EnumType enumType;
        private Map<Object, Object> enumConstants;
        private AttributeConverter converter;

        public FieldColumnInfo(Field field) {
            this.field = field;
            this.fieldType = field.getType();

            // remap safe conversions
            if (fieldType == java.util.Date.class) {
                fieldType = Timestamp.class;
            } else if (fieldType == int.class) {
                fieldType = Integer.class;
            } else if (fieldType == long.class) {
                fieldType = Long.class;
            }
        }

        <T extends Enum<?>> void setEnumConstants(EnumType type) {
            this.enumType = type;
            enumConstants = new HashMap<>();
            @SuppressWarnings("unchecked")
            T[] enums = (T[]) field.getType().getEnumConstants();
            for (T enumConst : enums) {
                Object key = (type == EnumType.ORDINAL ? enumConst.ordinal() : enumConst.name());
                enumConstants.put(key, enumConst);
            }
        }

        @Override
        public String toString() {
            return field.getName() + "->" + columnName;
        }

        public AttributeConverter getConverter() {
            return converter;
        }

        public void setConverter(AttributeConverter converter) {
            this.converter = converter;
        }
    }
}
