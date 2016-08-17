package org.jnaalisv.sqlmapper;

@FunctionalInterface
public interface SqlProducer {

    String produce() throws Exception;
}
