package org.jnaalisv.sqlmapper.internal;

public class VersionConflictException extends RuntimeException {

    public VersionConflictException(Class<?> entityClass, long id, long version) {
        super("UPDATE " + entityClass.getSimpleName() + ", id="+id+ ", version="+version + " failed.");
    }
}
