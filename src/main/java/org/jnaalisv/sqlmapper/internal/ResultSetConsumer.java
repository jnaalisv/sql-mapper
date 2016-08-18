package org.jnaalisv.sqlmapper.internal;

import java.sql.ResultSet;

@FunctionalInterface
public interface ResultSetConsumer<T> {

    T consume(ResultSet resultSet) throws Exception;
}
