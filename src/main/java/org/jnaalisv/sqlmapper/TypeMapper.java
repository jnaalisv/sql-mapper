package org.jnaalisv.sqlmapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class TypeMapper {

    public static final Object mapSqlType(Object object, int sqlType) {
        switch (sqlType) {
            case Types.TIMESTAMP:
                if (object instanceof Timestamp) {
                    return object;
                }
                if (object instanceof java.util.Date) {
                    return new Timestamp(((java.util.Date) object).getTime());
                }
                if (object instanceof LocalDateTime) {
                    LocalDateTime localDateTime = (LocalDateTime) object;
                    return Timestamp.valueOf(localDateTime);
                }
                break;

            case Types.DATE:
                if (object instanceof LocalDate) {
                    LocalDate localDate = (LocalDate) object;
                    return Date.valueOf(localDate);
                }
                break;

            case Types.TIME:
                if (object instanceof LocalTime) {
                    LocalTime localTime = (LocalTime) object;
                    return Time.valueOf(localTime);
                }
                break;

            case Types.DECIMAL:
                if (object instanceof BigInteger) {
                    return new BigDecimal(((BigInteger) object));
                }
                break;

            case Types.SMALLINT:
                if (object instanceof Boolean) {
                    return (((Boolean) object) ? (short) 1 : (short) 0);
                }
                break;

            default:
                break;
        }
        return object;
    }
}
