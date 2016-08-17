package org.jnaalisv.sqlmapper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;

public class FailFastOnResourceLeakConnectionProxy implements InvocationHandler {

    private final ArrayList<Statement> statements;
    private final Connection delegate;

    private FailFastOnResourceLeakConnectionProxy(Connection delegate) {
        this.delegate = delegate;
        this.statements = new ArrayList<>();
    }

    public static Connection wrapConnection(final Connection delegate) {
        FailFastOnResourceLeakConnectionProxy handler = new FailFastOnResourceLeakConnectionProxy(delegate);
        return (Connection) Proxy.newProxyInstance(FailFastOnResourceLeakConnectionProxy.class.getClassLoader(), new Class[]{Connection.class}, handler);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if ("close".equals(method.getName())) {
            try {
                for (Statement stmt : statements) {
                    if (stmt.isClosed()) {

                    } else {
                        stmt.close();
                        throw new RuntimeException(stmt + " was open!");
                    }
                }
            } finally {
                statements.clear();
            }
        }

        final Object ret = method.invoke(delegate, args);

        if (ret instanceof Statement) {
            statements.add((Statement) ret);
        }

        return ret;
    }
}
