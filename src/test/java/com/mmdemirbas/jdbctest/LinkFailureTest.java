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

    @TestFactory
    Stream<DynamicTest> tryFixedTimes() throws IOException {
        long repeatCount = 10L;

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
        Path configFile = Paths.get("db-properties.json");

        String text      = new String(Files.readAllBytes(configFile), StandardCharsets.UTF_8);
        DbDoc  json      = JsonParser.parseDoc(text);
        String host      = ((JsonString) json.get("host")).getString();
        String username  = ((JsonString) json.get("username")).getString();
        String password  = ((JsonString) json.get("password")).getString();
        String database  = ((JsonString) json.get("database")).getString();
        String port      = ((JsonString) json.get("port")).getString();
        String urlFormat = "jdbc:mysql://{0}:{1}/{2}?zeroDateTimeBehavior=convertToNull&tcpKeepAlive=true&useSSL=true&requireSSL=true&trustServerCertificate=true&allowLocalInfile=false&useBatchMultiSend=false&sessionVariables=wait_timeout=3600";
        String url       = MessageFormat.format(urlFormat, host, port, database);

        AtomicInteger failCount = new AtomicInteger();
        return LongStream.range(0L, repeatCount + 1)
                         .mapToObj(index -> {
                             if (index < repeatCount) {
                                 String displayName = String.format("[%d]", index);
                                 return DynamicTest.dynamicTest(displayName, () -> {
                                     try (Connection connection = DriverManager.getConnection(url, username, password);
                                          Statement statement = connection.createStatement();
                                          ResultSet resultSet = statement.executeQuery("SELECT 1")) {
                                         resultSet.next();
                                         resultSet.getString(1);
                                     } catch (Throwable e) {
                                         failCount.incrementAndGet();
                                         fail(e);
                                     }
                                 });
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
    Stream<DynamicTest> tryUntilFail() throws IOException {
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
        Path configFile = Paths.get("db-properties.json");

        String text      = new String(Files.readAllBytes(configFile), StandardCharsets.UTF_8);
        DbDoc  json      = JsonParser.parseDoc(text);
        String host      = ((JsonString) json.get("host")).getString();
        String username  = ((JsonString) json.get("username")).getString();
        String password  = ((JsonString) json.get("password")).getString();
        String database  = ((JsonString) json.get("database")).getString();
        String port      = ((JsonString) json.get("port")).getString();
        String urlFormat = "jdbc:mysql://{0}:{1}/{2}?zeroDateTimeBehavior=convertToNull&tcpKeepAlive=true&useSSL=true&requireSSL=true&trustServerCertificate=true&allowLocalInfile=false&useBatchMultiSend=false&sessionVariables=wait_timeout=3600";
        String url       = MessageFormat.format(urlFormat, host, port, database);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<DynamicTest>() {
            private final AtomicBoolean stop = new AtomicBoolean();
            private final AtomicInteger index = new AtomicInteger();

            @Override
            public boolean hasNext() {
                return !stop.get();
            }

            @Override
            public DynamicTest next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                String displayName = String.format("[%d]", index.getAndIncrement());
                return DynamicTest.dynamicTest(displayName, () -> {
                    try (Connection connection = DriverManager.getConnection(url, username, password);
                         Statement statement = connection.createStatement();
                         ResultSet resultSet = statement.executeQuery("SELECT 1")) {
                        resultSet.next();
                        resultSet.getString(1);
                    } catch (Throwable e) {
                        stop.set(true);
                        fail(e);
                    }
                });
            }
        }, ORDERED & DISTINCT & NONNULL), false);
    }
}
