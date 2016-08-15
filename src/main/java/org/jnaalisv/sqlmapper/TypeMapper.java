package org.jnaalisv.sqlmapper;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
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

    public static String readClob(Clob clob) throws IOException, SQLException {

        try (Reader reader = clob.getCharacterStream()) {
            StringBuilder stringBuilder = new StringBuilder();
            char[] charBuffer = new char[1024];
            while (true) {
                int charsRead = reader.read(charBuffer);
                if (charsRead == -1) {
                    break;
                }
                stringBuilder.append(charBuffer, 0, charsRead);
            }
            return stringBuilder.toString();
        }
    }
}
