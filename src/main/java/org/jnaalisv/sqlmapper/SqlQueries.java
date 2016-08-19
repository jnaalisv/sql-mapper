package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.Introspected;
import com.zaxxer.sansorm.internal.Introspector;
import org.jnaalisv.sqlmapper.internal.ConnectionConsumer;
import org.jnaalisv.sqlmapper.internal.FailFastResourceProxy;
import org.jnaalisv.sqlmapper.internal.PreparedStatementConsumer;
import org.jnaalisv.sqlmapper.internal.ResultSetConsumer;
import org.jnaalisv.sqlmapper.internal.ResultSetToolBox;
import org.jnaalisv.sqlmapper.internal.StatementWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

public class SqlQueries {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlQueries.class);

    private final DataSource dataSource;

    public SqlQueries(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public final <T> T getConnection(ConnectionConsumer<T> connectionConsumer) {
        LOGGER.debug("getConnection");
        try (Connection connection = FailFastResourceProxy.wrap(dataSource.getConnection(), Connection.class) ) {
            return connectionConsumer.consume(connection);
        }
        catch (SQLException e) {
            if (e.getNextException() != null) {
                e = e.getNextException();
            }
            LOGGER.debug("SQLException ", e);
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            LOGGER.debug("Exception ", e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T prepareStatement(Connection connection, Callable<String> sqlBuilder, PreparedStatementConsumer<T> preparedStatementConsumer, Object... args) throws Exception {
        String sql = sqlBuilder.call();
        LOGGER.debug("prepareStatement "+ sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql) ) {

            if (args.length != 0 ) {
                StatementWrapper.populateStatementParameters(preparedStatement, args);
            }

            return preparedStatementConsumer.consume(preparedStatement);
        }
    }

    public static <T> T prepareStatementForInsert(Connection connection, Callable<String> sqlBuilder, String[] returnColumns, PreparedStatementConsumer<T> preparedStatementConsumer) throws Exception {
        String sql = sqlBuilder.call();
        LOGGER.debug("prepareStatementForInsert "+ sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql, returnColumns) ) {
            return preparedStatementConsumer.consume(preparedStatement);
        }
    }

    public static <T> T executeStatement(PreparedStatement preparedStatement, ResultSetConsumer<T> resultSetConsumer) throws Exception {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            return resultSetConsumer.consume(resultSet);
        }
    }

    // ---------------- //
    // Public Interface //
    // ---------------- //

    public <T> T execute(Callable<String> sqlProducer, ResultSetConsumer<T> resultSetConsumer, Object... args) {
        return getConnection(
                conn -> prepareStatement(
                        conn,
                        sqlProducer,
                        stmt -> executeStatement(stmt, resultSetConsumer),
                        args
                )
        );
    }

    public <T> T executeInsert(Callable<String> sqlProducer, String[] returnColumns, PreparedStatementConsumer<T> preparedStatementConsumer) {
        return getConnection(
                conn -> prepareStatementForInsert(
                        conn,
                        sqlProducer,
                        returnColumns,
                        preparedStatementConsumer
                )
        );
    }

    public <T> T executeUpdate(Callable<String> sqlProducer, PreparedStatementConsumer<T> preparedStatementConsumer, Object...args) {
        return getConnection(
                conn -> prepareStatement(
                        conn,
                        sqlProducer,
                        preparedStatementConsumer,
                        args
                )
        );
    }

    // -------------------- //
    //     List Queries     //
    // -------------------- //

    public final <T> List<T> list(Class<T> entityClass) {
        return execute(
                () -> CachingSqlStringBuilder.generateSelectFromClause(Introspector.getIntrospected(entityClass), null),
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass)
        );
    }

    public final <T> List<T> queryForList(Class<T> entityClass, String sql, Object... args) {
        return execute(
                () -> sql,
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass),
                args
        );
    }

    public final <T> List<T> queryForList(Callable<String> sqlProducer, Class<T> entityClass, Object... args) {
        return execute(
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
        return execute(
                () -> sql,
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass),
                args

        );
    }

    // -------------------- //
    //    Object Queries    //
    // -------------------- //

    public final <T> Optional<T> query(Callable<String> sqlProducer, Class<T> entityClass, Object... args) {
        return execute(
                sqlProducer,
                resultSet -> ResultSetToolBox.resultSetToObject(resultSet, entityClass),
                args
        );
    }

    public <T> Optional<T> getObjectById(Class<T> entityClass, Object... ids) {
        return execute(
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
        return execute(
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
        return getConnection(
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

        return prepareStatementForInsert(
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
        return getConnection(connection -> insertListBatched(connection, iterable));
    }

    public static <T> int insertListNotBatched(Connection connection, Iterable<T> iterable) throws Exception {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return 0;
        }

        T target = iterableIterator.next();

        Introspected introspected = Introspector.getIntrospected(target.getClass());
        String[] returnColumns = introspected.getGeneratedIdColumnNames();

        return prepareStatementForInsert(
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
        return getConnection(connection -> insertListNotBatched(connection, iterable));
    }

    // -------------------- //
    //  Update Statements   //
    // -------------------- //

    public <T> int updateObject(T target) {
        return executeUpdate(
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
        return executeUpdate(
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

        return executeUpdate(
                () -> CachingSqlStringBuilder.deleteObjectByIdSql(Introspector.getIntrospected(clazz)),
                PreparedStatement::executeUpdate,
                objectIds
        );
    }
}
