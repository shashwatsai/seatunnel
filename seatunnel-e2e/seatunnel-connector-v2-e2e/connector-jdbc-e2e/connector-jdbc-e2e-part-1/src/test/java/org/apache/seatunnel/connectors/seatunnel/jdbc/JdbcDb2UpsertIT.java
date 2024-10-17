/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class JdbcDb2UpsertIT extends TestSuiteBase implements TestResource {

    private static final String DB2_CONTAINER_HOST = "db2-e2e";

    private static final String DB2_DATABASE = "E2E";
    private static final String DB2_SOURCE = "SOURCE";
    private static final String DB2_SINK = "SINK";

    private static final Pair<String[], List<SeaTunnelRow>> TEST_DATASET = generateTestDataset();

    /** <a href="https://hub.docker.com/r/ibmcom/db2">db2 in dockerhub</a> */
    private static final String DB2_IMAGE = "ibmcom/db2";

    private static final int PORT = 50000;
    private static final int LOCAL_PORT = 50000;
    private static final String DB2_USER = "db2inst1";
    private static final String DB2_PASSWORD = "123456";

    private static final String CREATE_SQL_SOURCE =
            "create table %s\n"
                    + "(\n"
                    + "    C_BOOLEAN          BOOLEAN,\n"
                    + "    C_SMALLINT         SMALLINT,\n"
                    + "    C_INT              INTEGER NOT NULL PRIMARY KEY,\n"
                    + "    C_INTEGER          INTEGER,\n"
                    + "    C_BIGINT           BIGINT,\n"
                    + "    C_DECIMAL          DECIMAL(5),\n"
                    + "    C_DEC              DECIMAL(5),\n"
                    + "    C_NUMERIC          DECIMAL(5),\n"
                    + "    C_NUM              DECIMAL(5),\n"
                    + "    C_REAL             REAL,\n"
                    + "    C_FLOAT            DOUBLE,\n"
                    + "    C_DOUBLE           DOUBLE,\n"
                    + "    C_DOUBLE_PRECISION DOUBLE,\n"
                    + "    C_CHAR             CHARACTER(1),\n"
                    + "    C_VARCHAR          VARCHAR(255),\n"
                    + "    C_BINARY           BINARY(1),\n"
                    + "    C_VARBINARY        VARBINARY(2048),\n"
                    + "    C_DATE             DATE\n"
                    + ");\n";

    private static final String CREATE_SQL_SINK =
            "create table %s\n"
                    + "(\n"
                    + "    C_BOOLEAN          BOOLEAN,\n"
                    + "    C_SMALLINT         SMALLINT,\n"
                    + "    C_INT              INTEGER NOT NULL PRIMARY KEY,\n"
                    + "    C_INTEGER          INTEGER,\n"
                    + "    C_BIGINT           BIGINT,\n"
                    + "    C_DECIMAL          DECIMAL(5),\n"
                    + "    C_DEC              DECIMAL(5),\n"
                    + "    C_NUMERIC          DECIMAL(5),\n"
                    + "    C_NUM              DECIMAL(5),\n"
                    + "    C_REAL             REAL,\n"
                    + "    C_FLOAT            DOUBLE,\n"
                    + "    C_DOUBLE           DOUBLE,\n"
                    + "    C_DOUBLE_PRECISION DOUBLE,\n"
                    + "    C_CHAR             CHARACTER(1),\n"
                    + "    C_VARCHAR          VARCHAR(255),\n"
                    + "    C_BINARY           BINARY(1),\n"
                    + "    C_VARBINARY        VARBINARY(2048),\n"
                    + "    C_DATE             DATE,\n"
                    + "    C_UPDATED_AT       TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
                    + ");\n";

    private static final String CREATE_TRIGGER_SQL =
            "CREATE TRIGGER c_updated_at_trigger\n"
                    + "    BEFORE UPDATE ON %s\n"
                    + "    REFERENCING NEW AS new_row\n"
                    + "    FOR EACH ROW\n"
                    + "BEGIN ATOMIC\n"
                    + "SET new_row.c_updated_at = CURRENT_TIMESTAMP;\n"
                    + "END;";

    private Db2Container db2Container;
    private Connection connection;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + driverUrl()
                                        + " --no-check-certificate");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/ibm/db2/jcc/db2jcc/db2jcc4/db2jcc-db2jcc4.jar";
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        db2Container = startDb2Container().withImagePullPolicy(PullPolicy.alwaysPull());
        Startables.deepStart(Stream.of(db2Container)).join();
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);

        createTable(CREATE_SQL_SOURCE, DB2_SOURCE);
        createTable(CREATE_SQL_SINK, DB2_SINK);
        createTrigger(DB2_SINK);
        initSourceTablesData();
    }

    private void initializeJdbcConnection() {
        try {
            connection = db2Container.createConnection("");
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @TestTemplate
    public void testDb2UpsertE2e(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        clearTable(buildTableInfoWithSchema(DB2_DATABASE, DB2_SINK));

        Container.ExecResult execResult = container.executeJob("/jdbc_db2_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        Assertions.assertIterableEquals(
                query(
                        String.format(
                                "SELECT C_BOOLEAN, C_SMALLINT, C_INT, C_INTEGER, C_BIGINT, C_DECIMAL, C_DEC, C_NUMERIC, C_NUM, C_REAL, C_FLOAT, C_DOUBLE, C_DOUBLE_PRECISION, C_CHAR, C_VARCHAR, C_BINARY, C_VARBINARY, C_DATE  FROM %s",
                                buildTableInfoWithSchema(DB2_DATABASE, DB2_SOURCE))),
                query(
                        String.format(
                                "SELECT C_BOOLEAN, C_SMALLINT, C_INT, C_INTEGER, C_BIGINT, C_DECIMAL, C_DEC, C_NUMERIC, C_NUM, C_REAL, C_FLOAT, C_DOUBLE, C_DOUBLE_PRECISION, C_CHAR, C_VARCHAR, C_BINARY, C_VARBINARY, C_DATE  FROM %s",
                                buildTableInfoWithSchema(DB2_DATABASE, DB2_SINK))));
    }

    private void clearTable(String table) throws SQLException {
        String sql = "truncate table " + buildTableInfoWithSchema(DB2_DATABASE, table);
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private List<List<Object>> query(String sql) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getString(i));
                }
                result.add(objects);
                log.debug(String.format("Print query, sql: %s, data: %s", sql, objects));
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Db2Container startDb2Container() {
        Db2Container container =
                new Db2Container(DB2_IMAGE)
                        .withExposedPorts(PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DB2_CONTAINER_HOST)
                        .withDatabaseName(DB2_DATABASE)
                        .withUsername(DB2_USER)
                        .withPassword(DB2_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DB2_IMAGE)))
                        .acceptLicense();
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", LOCAL_PORT, PORT)));
        return container;
    }

    public String quoteIdentifier(String field) {
        return "`" + field + "`";
    }

    public String buildTableInfoWithSchema(String schema, String table) {
        if (StringUtils.isNotBlank(schema)) {
            return quoteIdentifier(schema) + "." + quoteIdentifier(table);
        } else {
            return quoteIdentifier(table);
        }
    }

    private void createTable(String createTableTemplate, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try {
                statement.execute(
                        String.format(
                                createTableTemplate,
                                buildTableInfoWithSchema(DB2_DATABASE, tableName)));
                connection.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void createTrigger(String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try {
                statement.execute(
                        String.format(
                                JdbcDb2UpsertIT.CREATE_TRIGGER_SQL,
                                buildTableInfoWithSchema(DB2_DATABASE, tableName)));
                connection.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void initSourceTablesData() throws SQLException {
        String columns = Arrays.stream(TEST_DATASET.getLeft()).collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(TEST_DATASET.getLeft())
                        .map(f -> "?")
                        .collect(Collectors.joining(", "));
        String sql =
                "INSERT INTO "
                        + buildTableInfoWithSchema(DB2_DATABASE, DB2_SOURCE)
                        + " ("
                        + columns
                        + " ) VALUES ("
                        + placeholders
                        + ")";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (SeaTunnelRow row : TEST_DATASET.getRight()) {
                for (int i = 0; i < row.getArity(); i++) {
                    statement.setObject(i + 1, row.getField(i));
                }
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (db2Container != null) {
            db2Container.close();
        }
    }

    private static Pair<String[], List<SeaTunnelRow>> generateTestDataset() {
        String[] fieldNames = {
            "C_BOOLEAN",
            "C_SMALLINT",
            "C_INT",
            "C_INTEGER",
            "C_BIGINT",
            "C_DECIMAL",
            "C_DEC",
            "C_NUMERIC",
            "C_NUM",
            "C_REAL",
            "C_FLOAT",
            "C_DOUBLE",
            "C_DOUBLE_PRECISION",
            "C_CHAR",
            "C_VARCHAR",
            "C_BINARY",
            "C_VARBINARY",
            "C_DATE"
        };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i % 2 == 0 ? Boolean.TRUE : Boolean.FALSE,
                                Short.valueOf("1"),
                                i,
                                i,
                                Long.parseLong("1"),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 18),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                Double.parseDouble("1.1"),
                                Double.parseDouble("1.1"),
                                "f",
                                String.format("f1_%s", i),
                                "f".getBytes(),
                                "test".getBytes(),
                                Date.valueOf(LocalDate.now())
                            });
            rows.add(row);
        }
        return Pair.of(fieldNames, rows);
    }
}
