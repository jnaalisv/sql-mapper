package org.jnaalisv.sqlmapper;

import java.sql.Connection;

@FunctionalInterface
public interface ConnectionConsumer<T> {

    T consume(Connection connection) throws Exception;
}
