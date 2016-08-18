package org.jnaalisv.sqlmapper.internal;

@FunctionalInterface
public interface SqlProducer {

    String produce() throws Exception;
}
