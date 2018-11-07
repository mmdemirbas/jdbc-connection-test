package com.mmdemirbas.jdbctest;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.JsonParser;
import com.mysql.cj.xdevapi.JsonString;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.opentest4j.MultipleFailuresError;

import javax.sql.ConnectionPoolDataSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Muhammed Demirbaş
 * @since 2018-11-05 10:37
 */
final class DetailedTest {

    private static final int    REPEAT = 10;
    private static final Config CONFIG = Config.from(Paths.get("db-properties.json"));

    @Disabled("deprecated in favor of sequentialConnections_AsDynamicTests")
    @Test
    void sequentialConnections() {
        List<Throwable>     errors  = newList();
        Consumer<Throwable> onError = errors::add;
        runSequential(REPEAT, connect(withoutPool(), onError, query(onError)));
        assertNoError(errors, REPEAT);
    }

    @TestFactory
    List<DynamicTest> sequentialConnections_AsDynamicTests() {
        Consumer<Throwable> onError = e -> { throw new RuntimeException(e); };
        List<DynamicTest>   tests   = new ArrayList<>();
        for (int i = 0; i < REPEAT; i++) {
            tests.add(DynamicTest.dynamicTest(String.format("[%d]", i),
                                              connect(withoutPool(), onError, query(onError))::run));
        }
        return tests;
    }


    @TestFactory
    List<DynamicTest> sequentialConnectionsWithPool_AsDynamicTests() {
        Consumer<Throwable>      onError = e -> { throw new RuntimeException(e); };
        List<DynamicTest>        tests   = new ArrayList<>();
        ConnectionPoolDataSource pool    = CONFIG.newConnectionPool();
        for (int i = 0; i < REPEAT; i++) {
            tests.add(DynamicTest.dynamicTest(String.format("[%d]", i),
                                              connect(withPool(pool), onError, query(onError))::run));
        }
        return tests;
    }

    @Test
    void parallelConnections() {
        List<Throwable>     errors  = newList();
        Consumer<Throwable> onError = errors::add;
        List<Future<?>>     futures = runParallel(REPEAT, connect(withoutPool(), onError, query(onError)));
        wait(futures, onError);
        assertNoError(errors, REPEAT);
    }

    @Test
    void parallelConnections_WithConnectionPool() {
        List<Throwable>          errors  = newList();
        Consumer<Throwable>      onError = errors::add;
        ConnectionPoolDataSource pool    = CONFIG.newConnectionPool();
        List<Future<?>>          futures = runParallel(REPEAT, connect(withPool(pool), onError, query(onError)));
        wait(futures, onError);
        assertNoError(errors, REPEAT);
    }

    @Test
    void sequentialQueriesOverSingleConnection() {
        List<Throwable>     errors  = newList();
        Consumer<Throwable> onError = errors::add;
        connect(withoutPool(), onError, connection -> runSequential(REPEAT, query(onError, connection))).run();
        assertNoError(errors, REPEAT);
    }

    @Test
    void parallelQueriesOverSingleConnection() {
        List<Throwable>     errors  = newList();
        Consumer<Throwable> onError = errors::add;
        connect(withoutPool(),
                onError, connection -> wait(runParallel(REPEAT, query(onError, connection)), onError)).run();
        assertNoError(errors, REPEAT);
    }

    @Test
    void parallelQueriesOverParallelConnections() {
        int                 repeat  = (int) Math.sqrt(REPEAT);
        List<Throwable>     errors  = newList();
        Consumer<Throwable> onError = errors::add;
        List<Future<?>>     futures = new CopyOnWriteArrayList<>();
        futures.addAll(runParallel(repeat,
                                   connect(withoutPool(),
                                           onError,
                                           connection -> futures.addAll(runParallel(repeat,
                                                                                    query(onError, connection))))));
        wait(futures, onError);
        assertNoError(errors, repeat);
    }

    @Test
    void parallelQueriesOverParallelConnections_WithConnectionPool() {
        int                      repeat  = (int) Math.sqrt(REPEAT);
        List<Throwable>          errors  = newList();
        Consumer<Throwable>      onError = errors::add;
        ConnectionPoolDataSource pool    = CONFIG.newConnectionPool();
        List<Future<?>>          futures = new CopyOnWriteArrayList<>();
        futures.addAll(runParallel(repeat,
                                   connect(withPool(pool),
                                           onError,
                                           connection -> futures.addAll(runParallel(repeat,
                                                                                    query(onError, connection))))));
        wait(futures, onError);
        assertNoError(errors, repeat);
    }

    @Test
    void closedDueToInactivity() {
        int                 waitSeconds = 4;
        List<Throwable>     errors      = newList();
        Consumer<Throwable> onError     = errors::add;
        connect(withoutPool(), onError, connection -> {
            assertTimeoutPreemptively(Duration.ofSeconds(waitSeconds), () -> {
                while (!connection.isClosed()) {
                    Thread.sleep(REPEAT);
                }
            });
            assertTrue(returnOrThrow(connection::isClosed));
        }).run();
    }

    private static void assertNoError(List<Throwable> errors, int totalCount) {
        if (!errors.isEmpty()) {
            double errorRatio = ((double) errors.size()) / totalCount;
            throw new MultipleFailuresError(String.format("error ratio: %.2f%%", errorRatio), errors);
        }
    }

    private static Callable<Connection> withoutPool() {
        return CONFIG::newConnection;
    }

    private static Callable<Connection> withPool(ConnectionPoolDataSource pool) {
        return () -> pool.getPooledConnection()
                         .getConnection();
    }

    private static Runnable connect(Callable<Connection> connectionFactory,
                                    Consumer<Throwable> onError,
                                    Consumer<? super Connection> useConnection) {
        return () -> {
            try (Connection connection = connectionFactory.call()) {
                useConnection.accept(connection);
            } catch (Exception e) {
                onError.accept(e);
            }
        };
    }

    private static Runnable query(Consumer<Throwable> onError, Connection connection) {
        return () -> query(onError).accept(connection);
    }

    private static Consumer<Connection> query(Consumer<Throwable> onError) {
        return connection -> {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT 'success'")) {
                resultSet.next();
                resultSet.getString(1);
            } catch (Exception e) {
                onError.accept(e);
            }
        };
    }

    private static List<Future<?>> runParallel(int repeat, Runnable task) {
        List<Future<?>> futures = new CopyOnWriteArrayList<>();
        ExecutorService pool    = Executors.newCachedThreadPool();
        runSequential(repeat, () -> futures.add(pool.submit(task)));
        pool.shutdown();
        return futures;
    }

    private static void runSequential(int repeat, Runnable task) {
        int i = 0;
        System.out.printf("%d/%d...%n", i, repeat);
        while (i < repeat) {
            task.run();
            i++;
            System.out.printf("%d/%d%n", i, repeat);
        }
    }

    private static String readAll(Path path) {
        return returnOrThrow(() -> new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
    }

    private static void executeOrThrow(Executable throwingTask) {
        returnOrThrow(() -> {
            throwingTask.execute();
            return null;
        });
    }

    private static <T> T returnOrThrow(ThrowingSupplier<T> throwingTask) {
        try {
            return throwingTask.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Throwable> newList() {
        return new CopyOnWriteArrayList<>();
    }

    private static void wait(Iterable<? extends Future<?>> futures, Consumer<Throwable> onError) {
        for (Future<?> future : futures) {
            executeOrThrow(future::get);
        }
    }

    /**
     * @author Muhammed Demirbaş
     * @since 2018-11-06 10:05
     */
    static final class Config {

        private final String host;
        private final String username;
        private final String password;
        private final String database;
        private final String port;

        /**
         * Sample properties file content:
         * <pre>
         * {
         *   "host"     : "...",
         *   "username" : "...",
         *   "password" : "...",
         *   "database" : "...",
         *   "port"     : "3306"
         * }</pre>
         */
        static Config from(Path path) {
            String text     = readAll(path);
            DbDoc  json     = JsonParser.parseDoc(text);
            String host     = ((JsonString) json.get("host")).getString();
            String username = ((JsonString) json.get("username")).getString();
            String password = ((JsonString) json.get("password")).getString();
            String database = ((JsonString) json.get("database")).getString();
            String port     = ((JsonString) json.get("port")).getString();
            return new Config(host, username, password, database, port);
        }

        private Config(String host, String username, String password, String database, String port) {
            this.host = host;
            this.username = username;
            this.password = password;
            this.database = database;
            this.port = port;
        }

        ConnectionPoolDataSource newConnectionPool() {
            MysqlConnectionPoolDataSource dataSource = new MysqlConnectionPoolDataSource();
            dataSource.setURL(buildUrl());
            dataSource.setUser(username);
            dataSource.setPassword(password);
            executeOrThrow(() -> dataSource.setUseSSL(true));
            executeOrThrow(() -> dataSource.setSslMode("VERIFY_IDENTITY"));
            return dataSource;
        }

        Connection newConnection() throws SQLException {
            return DriverManager.getConnection(buildUrl(), username, password);
        }

        private String buildUrl() {
            return MessageFormat.format(
                    "jdbc:mysql://{0}:{1}/{2}?zeroDateTimeBehavior=convertToNull&tcpKeepAlive=true&useSSL=true&requireSSL=true&trustServerCertificate=true&allowLocalInfile=false&useBatchMultiSend=false&sessionVariables=wait_timeout=3600",
                    host,
                    port,
                    database);
        }
    }
}