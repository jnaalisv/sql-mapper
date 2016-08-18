package org.jnaalisv.sqlmapper.internal;

import java.sql.Connection;

@FunctionalInterface
public interface ConnectionConsumer<T> {

    T consume(Connection connection) throws Exception;
}
