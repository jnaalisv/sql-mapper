package org.jnaalisv.sqlmapper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class FailFastResourceProxy<T> implements InvocationHandler {

    private final ArrayList<Connection> connections = new ArrayList<>();
    private final ArrayList<Statement> statements = new ArrayList<>();
    private final ArrayList<ResultSet> resultSets = new ArrayList<>();
    private final T delegate;

    private FailFastResourceProxy(T delegate) {
        this.delegate = delegate;
    }

    public static <T> T wrap(final T delegate, Class<T> delegateClass) {
        FailFastResourceProxy<T> handler = new FailFastResourceProxy<>(delegate);
        return (T) Proxy.newProxyInstance(FailFastResourceProxy.class.getClassLoader(), new Class[]{delegateClass}, handler);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if ("close".equals(method.getName())) {
            try {
                for (Statement stmt : statements) {
                    if (!stmt.isClosed()) {
                        stmt.close();
                        throw new RuntimeException(stmt + " was open!");
                    }
                }
            } finally {
                statements.clear();
            }

            try {
                for (Connection stmt : connections) {
                    if (!stmt.isClosed()) {
                        stmt.close();
                        throw new RuntimeException(stmt + " was open!");
                    }
                }
            } finally {
                connections.clear();
            }

            try {
                for (ResultSet stmt : resultSets) {
                    if (!stmt.isClosed()) {
                        stmt.close();
                        throw new RuntimeException(stmt + " was open!");
                    }
                }
            } finally {
                resultSets.clear();
            }
        }

        try {
            final Object ret = method.invoke(delegate, args);

            if (ret instanceof Statement) {
                statements.add((Statement) ret);
            } else if (ret instanceof Connection) {
                connections.add((Connection) ret);
            } else if (ret instanceof ResultSet) {
                resultSets.add((ResultSet) ret);
            }

            return ret;
        } catch(InvocationTargetException ite) {
            throw ite.getTargetException();
        }  catch(Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
