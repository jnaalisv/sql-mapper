package org.jnaalisv.sqlmapper.internal;

import com.zaxxer.sansorm.SqlFunction;
import com.zaxxer.sansorm.internal.OrmReader;
import com.zaxxer.sansorm.internal.OrmWriter;
import org.jnaalisv.sqlmapper.SqlQueries;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

@Deprecated
public class OldSqlWrapper {

    private final DataSource dataSource;

    public OldSqlWrapper(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public <T> Optional<T> getObjectById(Class<T> type, Object... ids) {
        return execute(connection -> OrmReader.objectById(connection, type, ids));
    }
    
    public <T> Optional<T> objectFromClause(Class<T> type, String clause, Object... args) {
        return execute(connection -> OrmReader.objectFromClause(connection, type, clause, args));
    }

    public <T> int insertObject(T object) {
        return execute(connection -> OrmWriter.insertObject(connection, object));
    }

    public <T> int updateObject(T object) {
        return execute(connection -> OrmWriter.updateObject(connection, object));
    }

    public <T> int deleteObject(T object) {
        return execute(connection ->  OrmWriter.deleteObject(connection, object));
    }

    public <T> int deleteObjectById(Class<T> clazz, Object... args) {
        return execute(connection -> OrmWriter.deleteObjectById(connection, clazz, args));
    }

    public <T> List<T> listFromClause(Class<T> clazz, String clause, Object... args) {
        return execute(connection -> OrmReader.listFromClause(connection, clazz, clause, args));
    }

    public <T> int countObjectsFromClause(Class<T> clazz, String clause, Object... args) {
        return execute(connection -> OrmReader.countObjectsFromClause(connection, clazz, clause, args));
    }

    public Optional<Number> numberFromSql(String sql, Object... args) {
        return execute(connection -> OrmReader.numberFromSql(connection, sql, args));
    }

    public int executeUpdate(final String sql, final Object... args) {
        return execute(connection ->
            SqlQueries.prepareStatement(
                    connection,
                    () -> sql,
                    PreparedStatement::executeUpdate,
                    args
        ));
    }

    public final <T> T execute(SqlFunction<T> sqlFunction) {
        try (Connection connection = FailFastResourceProxy.wrapConnection(dataSource.getConnection()) ) {
            return sqlFunction.execute(connection);
        }

        catch (SQLException e) {
            if (e.getNextException() != null) {
                e = e.getNextException();
            }
            throw new RuntimeException(e);
        }

        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> int[] insertListBatched(Iterable<T> iterable) {
        return execute(connection -> SqlQueries.insertListBatched(connection, iterable));
    }

    public <T> int insertListNotBatched(Iterable<T> iterable) {
        return execute(connection -> SqlQueries.insertListNotBatched(connection, iterable));
    }

    public <T> List<T> executeQuery(Class<T> entityClass, final String sql, final Object... args) {
        return execute(connection -> OrmReader.listFromQuery(connection, entityClass, sql, args));
    }
}
