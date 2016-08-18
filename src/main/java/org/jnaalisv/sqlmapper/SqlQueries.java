package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.Introspector;

import java.util.List;
import java.util.Optional;

public class SqlQueries {

    private SqlExecutor sqlExecutor;

    public SqlQueries(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    public final <T> List<T> list(Class<T> entityClass) {
        return sqlExecutor.execute(
                () -> CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(entityClass), null),
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass)
        );
    }

    public <T> List<T> queryForList(Class<T> entityClass, String sql, Object... args) {
        return sqlExecutor.execute(
                () -> sql,
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass),
                args
        );
    }

    public final <T> List<T> queryForList(SqlProducer sqlProducer, Class<T> entityClass, Object... args) {
        return sqlExecutor.execute(
                sqlProducer,
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass),
                args
        );
    }

    public <T> List<T> listFromClause(Class<T> entityClass, String clause, Object... args) {
        return queryForList(
                () -> CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(entityClass), clause),
                entityClass,
                args
        );
    }

    public final <T> Optional<T> query(SqlProducer sqlProducer, Class<T> entityClass, Object... args) {
        return sqlExecutor.execute(
                sqlProducer,
                resultSet -> ResultSetToolBox.resultSetToObject(resultSet, entityClass),
                args
        );
    }

    public <T> Optional<T> getObjectById(Class<T> entityClass, Object... ids) {
        return sqlExecutor.execute(
                () -> CachingSqlGenerator.getObjectByIdSql(entityClass),
                resultSet -> ResultSetToolBox.resultSetToObject(resultSet, entityClass),
                ids
        );
    }

    public final <T> Optional<T> query(String sql, Class<T> entityClass, Object... args) {
        return query(
                () -> sql,
                entityClass,
                args
        );
    }

    public <T> Optional<T> objectFromClause(Class<T> entityClass, String clause, Object... args) {
        return query(
                () -> CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(entityClass), clause),
                entityClass,
                args
        );
    }

    public Optional<Number> numberFromSql(SqlProducer sqlProducer, Object... args) {
        return sqlExecutor.execute(
                sqlProducer,
                resultSet -> {
                    if (resultSet.next()) {
                        return Optional.of( (Number) resultSet.getObject(1));
                    }
                    return Optional.empty();
                },
                args
        );
    }

    public <T> int countObjectsFromClause(Class<T> clazz, String clause, Object... args) {
        Optional<Number> maybeNumber =
                numberFromSql(
                        () -> CachingSqlGenerator.countObjectsFromClause(Introspector.getIntrospected(clazz), clause),
                        args);

        return maybeNumber
                .orElseThrow(() -> new RuntimeException("count query returned without results"))
                .intValue();
    }
}
