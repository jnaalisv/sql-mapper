package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.Introspected;
import com.zaxxer.sansorm.internal.Introspector;
import org.jnaalisv.sqlmapper.internal.ResultSetToolBox;
import org.jnaalisv.sqlmapper.internal.StatementWrapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

public class SqlQueries {

    private SqlExecutor sqlExecutor;

    public SqlQueries(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    // -------------------- //
    //     List Queries     //
    // -------------------- //

    public final <T> List<T> list(Class<T> entityClass) {
        return sqlExecutor.execute(
                () -> CachingSqlStringBuilder.generateSelectFromClause(Introspector.getIntrospected(entityClass), null),
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass)
        );
    }

    public final <T> List<T> queryForList(Class<T> entityClass, String sql, Object... args) {
        return sqlExecutor.execute(
                () -> sql,
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass),
                args
        );
    }

    public final <T> List<T> queryForList(Callable<String> sqlProducer, Class<T> entityClass, Object... args) {
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

    public <T> List<T> executeQuery(Class<T> entityClass, final String sql, final Object... args) {
        return sqlExecutor.execute(
                () -> sql,
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass),
                args

        );
    }

    // -------------------- //
    //    Object Queries    //
    // -------------------- //

    public final <T> Optional<T> query(Callable<String> sqlProducer, Class<T> entityClass, Object... args) {
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

    // -------------------- //
    //    Number Queries    //
    // -------------------- //

    public Optional<Number> numberFromSql(Callable<String> sqlProducer, Object... args) {
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

    // -------------------- //
    //  Insert Statements   //
    // -------------------- //

    public <T> int insertObject(T object) {
        return sqlExecutor.getConnection(
                connection -> {

                    Introspected introspected = Introspector.getIntrospected(object.getClass());
                    String[] returnColumns = introspected.getGeneratedIdColumnNames();
                    String sql = CachingSqlStringBuilder.createStatementForInsertSql(introspected);

                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql, returnColumns) ) {
                        return StatementWrapper.insert(preparedStatement, introspected, object);
                    }
                }
        );
    }

    public static <T> int[] insertListBatched(Connection connection, Iterable<T> iterable) throws Exception {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return new int[]{};
        }

        Introspected introspected = Introspector.getIntrospected(iterableIterator.next().getClass());
        String[] returnColumns = introspected.getGeneratedIdColumnNames();

        return SqlExecutor.prepareStatementForInsert(
                connection,
                () -> CachingSqlStringBuilder.createStatementForInsertSql(introspected),
                returnColumns,
                preparedStatement -> {
                    StatementWrapper statementWrapper = new StatementWrapper(preparedStatement);
                    for (T item : iterable) {
                        statementWrapper.addBatch(introspected, item);
                    }
                    return statementWrapper.executeBatch();
                }
        );
    }

    public <T> int[] insertListBatched(Iterable<T> iterable) {
        return sqlExecutor.getConnection(connection -> insertListBatched(connection, iterable));
    }

    public static <T> int insertListNotBatched(Connection connection, Iterable<T> iterable) throws Exception {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return 0;
        }

        T target = iterableIterator.next();

        Introspected introspected = Introspector.getIntrospected(target.getClass());
        String[] returnColumns = introspected.getGeneratedIdColumnNames();

        return SqlExecutor.prepareStatementForInsert(
                connection,
                () -> CachingSqlStringBuilder.createStatementForInsertSql(introspected),
                returnColumns,
                preparedStatement -> {
                    StatementWrapper statementWrapper = new StatementWrapper(preparedStatement);
                    for (T item : iterable) {
                        statementWrapper.insert(introspected, item);
                    }
                    return statementWrapper.getTotalRowCount();
                }
        );
    }

    public <T> int insertListNotBatched(Iterable<T> iterable) {
        return sqlExecutor.getConnection(connection -> insertListNotBatched(connection, iterable));
    }

    // -------------------- //
    //  Update Statements   //
    // -------------------- //

    public <T> int updateObject(T target) {
        return sqlExecutor.executeUpdate(
                () -> CachingSqlStringBuilder.createStatementForUpdateSql(Introspector.getIntrospected(target.getClass())),
                preparedStatement -> StatementWrapper.update(
                        preparedStatement,
                        Introspector.getIntrospected(target.getClass()),
                        target)
        );
    }

    // -------------------- //
    //  Delete Statements   //
    // -------------------- //

    public <T> int deleteObjectById(Class<T> clazz, Object... args) {
        return sqlExecutor.executeUpdate(
                () -> CachingSqlStringBuilder.deleteObjectByIdSql(Introspector.getIntrospected(clazz)),
                PreparedStatement::executeUpdate,
                args
        );
    }

    public <T> int deleteObject(T object, Class<T> clazz) {

        Object[] objectIds = null;

        try {
            objectIds = Introspector.getIntrospected(clazz).getActualIds(object);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }

        return sqlExecutor.executeUpdate(
                () -> CachingSqlStringBuilder.deleteObjectByIdSql(Introspector.getIntrospected(clazz)),
                PreparedStatement::executeUpdate,
                objectIds
        );
    }
}
