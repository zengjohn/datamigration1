package com.example.moveprog.util;

import java.sql.Types;

public final class DBUtils {

    private DBUtils() {
    }

    public static boolean isCharType(int sqlType) {
        if (sqlType == Types.CHAR || sqlType == Types.VARCHAR || sqlType == Types.NCHAR || sqlType == Types.NVARCHAR || sqlType == Types.LONGVARCHAR  || sqlType == Types.LONGNVARCHAR) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isNCharType(int sqlType) {
        return (sqlType == Types.NCHAR || sqlType == Types.NVARCHAR);
    }

    public static boolean isClobType(int sqlType) {
        return (sqlType == Types.CLOB || sqlType == Types.NCLOB);
    }

    public static boolean isNClobType(int sqlType) {
        return (sqlType == Types.NCLOB);
    }

    public static boolean isBitType(int sqlType) {
        if (sqlType == Types.BINARY || sqlType == Types.VARBINARY
                || sqlType == Types.LONGVARBINARY || sqlType == Types.BIT) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isBlobType(int sqlType) {
        if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.VARBINARY
                || sqlType == Types.LONGVARBINARY) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isNumber(int sqlType) {
        if (sqlType == Types.TINYINT || sqlType == Types.SMALLINT || sqlType == Types.INTEGER
                || sqlType == Types.BIGINT || sqlType == Types.NUMERIC || sqlType == Types.DECIMAL) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isGenerateInt(int sqlType) {
        if (sqlType == Types.TINYINT || sqlType == Types.SMALLINT || sqlType == Types.INTEGER
                || sqlType == Types.BIGINT) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isReal(int sqlType) {
        return sqlType == Types.FLOAT || sqlType == Types.DOUBLE || sqlType == Types.REAL;
    }

    public static boolean isDecimal(int sqlType) {
        if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isValidPrecisionScale(Long precision, Integer scale) {
        return precision != null && precision != 0 && scale != null;
    }

    public static boolean isValidPrecision(Long precision) {
        return precision != null && precision != 0;
    }

    public static boolean isDateType(int sqlType) {
        return sqlType == Types.DATE;
    }

    public static boolean isTimeType(int sqlType) {
        return sqlType == Types.TIME;
    }

    public static boolean isTimestamp(int sqlType) {
        if (sqlType == Types.TIMESTAMP || sqlType == Types.TIME_WITH_TIMEZONE || sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
            return true;
        } else {
            return false;
        }
    }
}