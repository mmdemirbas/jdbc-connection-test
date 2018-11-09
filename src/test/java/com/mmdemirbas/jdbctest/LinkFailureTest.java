package com.mmdemirbas.jdbctest;

import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.JsonParser;
import com.mysql.cj.xdevapi.JsonString;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Muhammed Demirbaş
 * @since 2018-11-06 11:01
 */
final class LinkFailureTest {

    /*
       Sample properties file content:

       {
         "host"     : "...",
         "username" : "...",
         "password" : "...",
         "database" : "...",
         "port"     : "3306"
       }
     */
    private static final Config CONFIG = parseConfig(Paths.get("db-properties.json"));

    @TestFactory
    Stream<DynamicTest> tryFixedTimes() {
        long repeatCount = 1000L;

        AtomicInteger failCount = new AtomicInteger();
        return LongStream.range(0L, repeatCount + 1)
                         .mapToObj(index -> {
                             if (index < repeatCount) {
                                 return CONFIG.newTest(index, failCount::incrementAndGet);
                             }

                             return DynamicTest.dynamicTest("summary", () -> {
                                 int failures = failCount.get();
                                 if (failures > 0) {
                                     fail(String.format("fail ratio: %.2f%% (%d failures)",
                                                        (failures * 100.0) / repeatCount,
                                                        failures));
                                 } else {
                                     System.out.printf("No failure after %d runs.%n", repeatCount);
                                 }
                             });
                         });
    }

    @TestFactory
    Stream<DynamicTest> tryUntilFail() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<DynamicTest>() {
            private final AtomicBoolean stop = new AtomicBoolean();
            private final AtomicInteger index = new AtomicInteger();

            @Override
            public DynamicTest next() {
                if (hasNext()) {
                    return CONFIG.newTest(index.getAndIncrement(), () -> stop.set(true));
                }
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return !stop.get();
            }
        }, ORDERED & DISTINCT & NONNULL), false);
    }

    private static Config parseConfig(Path configFile) {
        try {
            String text      = new String(Files.readAllBytes(configFile), StandardCharsets.UTF_8);
            DbDoc  json      = JsonParser.parseDoc(text);
            String host      = ((JsonString) json.get("host")).getString();
            String username  = ((JsonString) json.get("username")).getString();
            String password  = ((JsonString) json.get("password")).getString();
            String database  = ((JsonString) json.get("database")).getString();
            String port      = ((JsonString) json.get("port")).getString();
            String urlFormat = "jdbc:mysql://{0}:{1}/{2}?zeroDateTimeBehavior=convertToNull&tcpKeepAlive=true&useSSL=true&requireSSL=true&trustServerCertificate=true&allowLocalInfile=false&useBatchMultiSend=false&sessionVariables=wait_timeout=3600";
            String url       = MessageFormat.format(urlFormat, host, port, database);
            return new Config(url, username, password);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static final class Config {

        private final String url;
        private final String username;
        private final String password;

        Config(String url, String username, String password) {
            this.url = url;
            this.username = username;
            this.password = password;
        }

        DynamicTest newTest(long index, Runnable onFailCallback) {
            String displayName = String.format("[%d]", index);
            return DynamicTest.dynamicTest(displayName, () -> {
                long start = System.nanoTime();
                try (Connection connection = DriverManager.getConnection(url, username, password);
                     Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery("SELECT 1")) {
                    resultSet.next();
                    resultSet.getString(1);
                    logElapsedTime(start);
                } catch (Throwable e) {
                    logElapsedTime(start);
                    onFailCallback.run();
                    fail(e);
                }
            });
        }

        private static void logElapsedTime(long start) {
            System.out.println(MessageFormat.format("Elapsed time {0} nanos",
                                                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)));
        }
    }
}
