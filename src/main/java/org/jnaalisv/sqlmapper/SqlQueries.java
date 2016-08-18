package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.Introspected;
import com.zaxxer.sansorm.internal.Introspector;
import org.jnaalisv.sqlmapper.internal.ResultSetToolBox;
import org.jnaalisv.sqlmapper.internal.SqlProducer;
import org.jnaalisv.sqlmapper.internal.StatementWrapper;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;

public class SqlQueries {

    private SqlExecutor sqlExecutor;

    public SqlQueries(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    public final <T> List<T> list(Class<T> entityClass) {
        return sqlExecutor.execute(
                () -> CachingSqlStringBuilder.generateSelectFromClause(Introspector.getIntrospected(entityClass), null),
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
                () -> CachingSqlStringBuilder.generateSelectFromClause(Introspector.getIntrospected(entityClass), clause),
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
                () -> CachingSqlStringBuilder.getObjectByIdSql(entityClass),
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
                () -> CachingSqlStringBuilder.generateSelectFromClause(Introspector.getIntrospected(entityClass), clause),
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
                        () -> CachingSqlStringBuilder.countObjectsFromClause(Introspector.getIntrospected(clazz), clause),
                        args);

        return maybeNumber
                .orElseThrow(() -> new RuntimeException("count query returned without results"))
                .intValue();
    }

    public <T> T insertObject(T object) {
        return sqlExecutor.getConnection(
                connection -> {

                    Introspected introspected = Introspector.getIntrospected(object.getClass());
                    String[] returnColumns = introspected.getGeneratedIdColumnNames();
                    String sql = CachingSqlStringBuilder.createStatementForInsertSql(introspected);

                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql, returnColumns) ) {
                        int rowCount = StatementWrapper.insertOrUpdate(preparedStatement, introspected.getInsertableColumns(), introspected, object);

                        return object;
                    }
                }
        );
    }
}
