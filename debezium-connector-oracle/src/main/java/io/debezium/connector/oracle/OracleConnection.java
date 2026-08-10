/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.math.BigInteger;
import java.sql.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.util.Strings;

import oracle.jdbc.OracleTypes;
import oracle.sql.CharacterSet;

public class OracleConnection extends JdbcConnection {

    private final static Logger LOGGER = LoggerFactory.getLogger(OracleConnection.class);

    /**
     * Returned by column metadata in Oracle if no scale is set;
     */
    private static final int ORACLE_UNSET_SCALE = -127;

    /**
     * Pattern to identify system generated indices and column names.
     */
    private static final Pattern SYS_NC_PATTERN = Pattern.compile("^SYS_NC(?:_OID|_ROWINFO|[0-9][0-9][0-9][0-9][0-9])\\$$");

    /**
     * Pattern to identify abstract data type indices and column names.
     */
    private static final Pattern ADT_INDEX_NAMES_PATTERN = Pattern.compile("^\".*\"\\.\".*\".*");

    /**
     * Pattern to identify a hidden column based on redefining a table with the {@code ROWID} option.
     */
    private static final Pattern MROW_PATTERN = Pattern.compile("^M_ROW\\$\\$");

    /**
     * A field for the raw jdbc url. This field has no default value.
     */
    private static final Field URL = Field.create("url", "Raw JDBC url");

    /**
     * The database version.
     */
    private final OracleDatabaseVersion databaseVersion;

    private static final String QUOTED_CHARACTER = "\"";

    /**
     * Lazily-resolved database character set (NLS_CHARACTERSET), used to decode {@code HEXTORAW}
     * values for {@code CHAR}/{@code VARCHAR2} columns under the hybrid mining strategy.
     */
    private CharacterSet databaseCharacterSet;

    /**
     * Lazily-resolved national character set (NLS_NCHAR_CHARACTERSET), used to decode
     * {@code HEXTORAW} values for {@code NCHAR}/{@code NVARCHAR2} columns under the hybrid
     * mining strategy.
     */
    private CharacterSet nationalCharacterSet;

    public OracleConnection(JdbcConfiguration config, Supplier<ClassLoader> classLoaderSupplier) {
        this(config, classLoaderSupplier, true);
    }

    public OracleConnection(JdbcConfiguration config, Supplier<ClassLoader> classLoaderSupplier, boolean showVersion) {
        super(config, resolveConnectionFactory(config), classLoaderSupplier, QUOTED_CHARACTER, QUOTED_CHARACTER);
        this.databaseVersion = resolveOracleDatabaseVersion();
        if (showVersion) {
            LOGGER.info("Database Version: {}", databaseVersion.getBanner());
        }
    }

    public void setSessionToPdb(String pdbName) {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=" + pdbName);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    public void resetSessionToCdb() {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=cdb$root");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    public OracleDatabaseVersion getOracleVersion() {
        return databaseVersion;
    }

    private OracleDatabaseVersion resolveOracleDatabaseVersion() {
        String versionStr;
        try {
            try {
                // Oracle 18.1 introduced BANNER_FULL as the new column rather than BANNER
                // This column uses a different format than the legacy BANNER.
                versionStr = queryAndMap("SELECT BANNER_FULL FROM V$VERSION WHERE BANNER_FULL LIKE 'Oracle Database%'", (rs) -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                });
            }
            catch (SQLException e) {
                // exception ignored
                if (e.getMessage().contains("ORA-00904: \"BANNER_FULL\"")) {
                    LOGGER.debug("BANNER_FULL column not in V$VERSION, using BANNER column as fallback");
                    versionStr = null;
                }
                else {
                    throw e;
                }
            }

            // For databases prior to 18.1, a SQLException will be thrown due to BANNER_FULL not being a column and
            // this will cause versionStr to remain null, use fallback column BANNER for versions prior to 18.1.
            if (versionStr == null) {
                versionStr = queryAndMap("SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'Oracle Database%'", (rs) -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                });
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve Oracle database version", e);
        }

        if (versionStr == null) {
            throw new RuntimeException("Failed to resolve Oracle database version");
        }

        return OracleDatabaseVersion.parse(versionStr);
    }

    @Override
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
                                       String[] tableTypes)
            throws SQLException {

        Set<TableId> tableIds = super.readTableNames(null, schemaNamePattern, tableNamePattern, tableTypes);

        return tableIds.stream()
                .map(t -> new TableId(databaseCatalog, t.schema(), t.table()))
                .collect(Collectors.toSet());
    }

    /**
     * Retrieves all {@code TableId} in a given database catalog, filtering certain ids that
     * should be omitted from the returned set such as special spatial tables and index-organized
     * tables.
     *
     * @param catalogName the catalog/database name
     * @return set of all table ids for existing table objects
     * @throws SQLException if a database exception occurred
     */
    protected Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        final String query = "select owner, table_name from all_tables " +
        // filter special spatial tables
                "where table_name NOT LIKE 'MDRT_%' " +
                "and table_name NOT LIKE 'MDRS_%' " +
                "and table_name NOT LIKE 'MDXT_%' " +
                // filter index-organized-tables
                "and (table_name NOT LIKE 'SYS_IOT_OVER_%' and IOT_NAME IS NULL) " +
                // filter nested tables
                "and nested = 'NO'" +
                // filter parent tables of nested tables
                "and table_name not in (select PARENT_TABLE_NAME from ALL_NESTED_TABLES)";

        Set<TableId> tableIds = new HashSet<>();
        query(query, (rs) -> {
            while (rs.next()) {
                tableIds.add(new TableId(catalogName, rs.getString(1), rs.getString(2)));
            }
            LOGGER.trace("TableIds are: {}", tableIds);
        });

        return tableIds;
    }

    // todo replace metadata with something like this
    private ResultSet getTableColumnsInfo(String schemaNamePattern, String tableName) throws SQLException {
        String columnQuery = "select column_name, data_type, data_length, data_precision, data_scale, default_length, density, char_length from " +
                "all_tab_columns where owner like '" + schemaNamePattern + "' and table_name='" + tableName + "'";

        PreparedStatement statement = connection().prepareStatement(columnQuery);
        return statement.executeQuery();
    }

    // this is much faster, we will use it until full replacement of the metadata usage TODO
    public void readSchemaForCapturedTables(Tables tables, String databaseCatalog, String schemaNamePattern,
                                            ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc, Set<TableId> capturedTables)
            throws SQLException {

        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        DatabaseMetaData metadata = connection().getMetaData();
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        for (TableId tableId : capturedTables) {
            try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schemaNamePattern, tableId.table(), null)) {
                while (columnMetadata.next()) {
                    // add all whitelisted columns
                    readTableColumn(columnMetadata, tableId, columnFilter).ifPresent(column -> {
                        columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>())
                                .add(column.create());
                    });
                }
            }
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, null);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    @Override
    protected String resolveCatalogName(String catalogName) {
        final String pdbName = config().getString("pdb.name");
        return (!Strings.isNullOrEmpty(pdbName) ? pdbName : config().getString("dbname")).toUpperCase();
    }

    @Override
    public List<String> readTableUniqueIndices(DatabaseMetaData metadata, TableId id) throws SQLException {
        return super.readTableUniqueIndices(metadata, id.toDoubleQuoted());
    }

    @Override
    public Optional<Timestamp> getCurrentTimestamp() throws SQLException {
        return queryAndMap("SELECT CURRENT_TIMESTAMP FROM DUAL",
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1)) : Optional.empty());
    }

    @Override
    protected boolean isTableUniqueIndexIncluded(String indexName, String columnName) {
        if (columnName != null) {
            return !SYS_NC_PATTERN.matcher(columnName).matches()
                    && !ADT_INDEX_NAMES_PATTERN.matcher(columnName).matches()
                    && !MROW_PATTERN.matcher(columnName).matches();
        }
        return false;
    }

    /**
     * Get the current, most recent system change number.
     *
     * @return the current system change number
     * @throws SQLException if an exception occurred
     * @throws IllegalStateException if the query does not return at least one row
     */
    public Scn getCurrentScn() throws SQLException {
        return queryAndMap("SELECT CURRENT_SCN FROM V$DATABASE", (rs) -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getString(1));
            }
            throw new IllegalStateException("Could not get SCN");
        });
    }

    /**
     * Generate a given table's DDL metadata.
     *
     * @param tableId table identifier, should never be {@code null}
     * @return generated DDL
     * @throws SQLException if an exception occurred obtaining the DDL metadata
     * @throws NonRelationalTableException the table is not a relational table
     */
    public String getTableMetadataDdl(TableId tableId) throws SQLException, NonRelationalTableException {
        try {
            // This table contains all available objects that are considered relational & object based.
            // By querying for TABLE_TYPE is null, we are explicitly confirming what if an entry exists
            // that the table is in-fact a relational table and if the result set is empty, the object
            // is another type, likely an object-based table, in which case we cannot generate DDL.
            final String tableType = "SELECT COUNT(1) FROM ALL_ALL_TABLES WHERE OWNER='" + tableId.schema()
                    + "' AND TABLE_NAME='" + tableId.table() + "' AND TABLE_TYPE IS NULL";
            if (queryAndMap(tableType, rs -> rs.next() ? rs.getInt(1) : 0) == 0) {
                throw new NonRelationalTableException("Table " + tableId + " is not a relational table");
            }

            // The storage and segment attributes aren't necessary
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', false); end;");
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', false); end;");
            // In case DDL is returned as multiple DDL statements, this allows the parser to parse each separately.
            // This is only critical during streaming as during snapshot the table structure is built from JDBC driver queries.
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', true); end;");
            return queryAndMap("SELECT dbms_metadata.get_ddl('TABLE','" + tableId.table() + "','" + tableId.schema() + "') FROM DUAL", rs -> {
                if (!rs.next()) {
                    throw new DebeziumException("Could not get DDL metadata for table: " + tableId);
                }

                Object res = rs.getObject(1);
                return ((Clob) res).getSubString(1, (int) ((Clob) res).length());
            });
        }
        finally {
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'DEFAULT'); end;");
        }
    }

    /**
     * Get the current connection's session statistic by name.
     *
     * @param name the name of the statistic to be fetched, must not be {@code null}
     * @return the session statistic value, never {@code null}
     * @throws SQLException if an exception occurred obtaining the session statistic value
     */
    public Long getSessionStatisticByName(String name) throws SQLException {
        return queryAndMap("SELECT VALUE FROM v$statname n, v$mystat m WHERE n.name='" + name +
                "' AND n.statistic#=m.statistic#", rs -> rs.next() ? rs.getLong(1) : 0L);
    }

    /**
     * Returns whether the given table exists or not.
     *
     * @param tableName table name, should not be {@code null}
     * @return true if the table exists, false if it does not
     * @throws SQLException if a database exception occurred
     */
    public boolean isTableExists(String tableName) throws SQLException {
        return queryAndMap("SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME = '" + tableName + "'",
                rs -> rs.next() && rs.getLong(1) > 0);
    }

    public boolean isTableExists(TableId tableId) throws SQLException {
        return queryAndMap("SELECT COUNT(1) FROM ALL_TABLES WHERE OWNER = '" + tableId.schema() + "' AND TABLE_NAME = '" + tableId.table() + "'",
                rs -> rs.next() && rs.getLong(1) > 0);
    }

    /**
     * Returns whether the given table is empty or not.
     *
     * @param tableName table name, should not be {@code null}
     * @return true if the table has no records, false otherwise
     * @throws SQLException if a database exception occurred
     */
    public boolean isTableEmpty(String tableName) throws SQLException {
        return getRowCount(tableName) == 0L;
    }

    /**
     * Returns the number of rows in a given table.
     *
     * @param tableName table name, should not be {@code null}
     * @return the number of rows
     * @throws SQLException if a database exception occurred
     */
    public long getRowCount(String tableName) throws SQLException {
        return queryAndMap("SELECT COUNT(1) FROM " + tableName, rs -> {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0L;
        });
    }

    public <T> T singleOptionalValue(String query, ResultSetExtractor<T> extractor) throws SQLException {
        return queryAndMap(query, rs -> rs.next() ? extractor.apply(rs) : null);
    }

    @Override
    public String buildSelectWithRowLimits(TableId tableId,
                                           int limit,
                                           String projection,
                                           Optional<String> condition,
                                           String orderBy) {
        final TableId table = new TableId(null, tableId.schema(), tableId.table());
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql
                .append(projection)
                .append(" FROM ");
        sql.append(quotedTableIdString(table));
        if (condition.isPresent()) {
            sql
                    .append(" WHERE ")
                    .append(condition.get());
        }
        if (getOracleVersion().getMajor() < 12) {
            sql
                    .insert(0, " SELECT * FROM (")
                    .append(" ORDER BY ")
                    .append(orderBy)
                    .append(")")
                    .append(" WHERE ROWNUM <=")
                    .append(limit);
        }
        else {
            sql
                    .append(" ORDER BY ")
                    .append(orderBy)
                    .append(" FETCH NEXT ")
                    .append(limit)
                    .append(" ROWS ONLY");
        }
        return sql.toString();
    }

    public static String connectionString(JdbcConfiguration config) {
        return config.getString(URL) != null ? config.getString(URL)
                : ConnectorAdapter.parse(config.getString("connection.adapter")).getConnectionUrl();
    }

    private static ConnectionFactory resolveConnectionFactory(JdbcConfiguration config) {
        return JdbcConnection.patternBasedFactory(connectionString(config));
    }

    /**
     * Determine whether the Oracle server has the archive log enabled.
     *
     * @return {@code true} if the server's {@code LOG_MODE} is set to {@code ARCHIVELOG}, or {@code false} otherwise
     */
    protected boolean isArchiveLogMode() {
        try {
            final String mode = queryAndMap("SELECT LOG_MODE FROM V$DATABASE", rs -> rs.next() ? rs.getString(1) : "");
            LOGGER.debug("LOG_MODE={}", mode);
            return "ARCHIVELOG".equalsIgnoreCase(mode);
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to Oracle and looking at LOG_MODE mode: ", e);
        }
    }

    /**
     * Resolve a system change number to a timestamp, return value is in database timezone.
     *
     * The SCN to TIMESTAMP mapping is only retained for the duration of the flashback query area.
     * This means that eventually the mapping between these values are no longer kept by Oracle
     * and making a call with a SCN value that has aged out will result in an ORA-08181 error.
     * This function explicitly checks for this use case and if a ORA-08181 error is thrown, it is
     * therefore treated as if a value does not exist returning an empty optional value.
     *
     * @param scn the system change number, must not be {@code null}
     * @return an optional timestamp when the system change number occurred
     * @throws SQLException if a database exception occurred
     */
    public Optional<OffsetDateTime> getScnToTimestamp(Scn scn) throws SQLException {
        try {
            return queryAndMap("SELECT scn_to_timestamp('" + scn + "') FROM DUAL", rs -> rs.next()
                    ? Optional.of(rs.getObject(1, OffsetDateTime.class))
                    : Optional.empty());
        }
        catch (SQLException e) {
            if (e.getMessage().startsWith("ORA-08181")) {
                // ORA-08181 specified number is not a valid system change number
                // This happens when the SCN provided is outside the flashback area range
                // This should be treated as a value is not available rather than an error
                return Optional.empty();
            }
            // Any other SQLException should be thrown
            throw e;
        }
    }

    @Override
    protected ColumnEditor overrideColumn(ColumnEditor column) {
        // This allows the column state to be overridden before default-value resolution so that the
        // output of the default value is within the same precision as that of the column values.
        if (OracleTypes.TIMESTAMP == column.jdbcType()) {
            column.length(column.scale().orElse(Column.UNSET_INT_VALUE)).scale(null);
        }
        else if (OracleTypes.NUMBER == column.jdbcType()) {
            column.scale().filter(s -> s == ORACLE_UNSET_SCALE).ifPresent(s -> column.scale(null));
        }
        return column;
    }

    @Override
    protected Map<TableId, List<Column>> getColumnsDetails(String databaseCatalog,
                                                           String schemaNamePattern,
                                                           String tableName,
                                                           Tables.TableFilter tableFilter,
                                                           ColumnNameFilter columnFilter,
                                                           DatabaseMetaData metadata,
                                                           Set<TableId> viewIds)
            throws SQLException {
        // The Oracle JDBC driver expects that if the table name contains a "/" character that
        // the table name is pre-escaped prior to the JDBC driver call, or else it throws an
        // exception about the character sequence being improperly escaped.
        if (tableName != null && tableName.contains("/")) {
            tableName = tableName.replace("/", "//");
        }
        return super.getColumnsDetails(databaseCatalog, schemaNamePattern, tableName, tableFilter, columnFilter, metadata, viewIds);
    }

    /**
     * An exception that indicates the operation failed because the table is not a relational table.
     */
    public static class NonRelationalTableException extends DebeziumException {
        public NonRelationalTableException(String message) {
            super(message);
        }
    }

    public void removeAllLogFilesFromLogMinerSession() throws SQLException {
        final Set<String> fileNames = queryAndMap("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS", rs -> {
            final Set<String> results = new HashSet<>();
            while (rs.next()) {
                results.add(rs.getString(1));
            }
            return results;
        });

        for (String fileName : fileNames) {
            LOGGER.debug("Removing file {} from LogMiner mining session.", fileName);
            final String sql = "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '" + fileName + "');END;";
            try (CallableStatement statement = connection(false).prepareCall(sql)) {
                statement.execute();
            }
        }
    }

    public RedoThreadState getRedoThreadState() throws SQLException {
        final String query = "SELECT * FROM V$THREAD";
        try {
            return queryAndMap(query, rs -> {
                RedoThreadState.Builder builder = RedoThreadState.builder();
                while (rs.next()) {
                    // While this field should never be NULL, the database metadata allows it
                    final int threadId = rs.getInt("THREAD#");
                    if (!rs.wasNull()) {
                        RedoThreadState.RedoThread.Builder threadBuilder = builder.thread()
                                .threadId(threadId)
                                .status(rs.getString("STATUS"))
                                .enabled(rs.getString("ENABLED"))
                                .logGroups(rs.getLong("GROUPS"))
                                .instanceName(rs.getString("INSTANCE"))
                                .openTime(readTimestampAsInstant(rs, "OPEN_TIME"))
                                .currentGroupNumber(rs.getLong("CURRENT_GROUP#"))
                                .currentSequenceNumber(rs.getLong("SEQUENCE#"))
                                .checkpointScn(readScnColumnAsScn(rs, "CHECKPOINT_CHANGE#"))
                                .checkpointTime(readTimestampAsInstant(rs, "CHECKPOINT_TIME"))
                                .enabledScn(readScnColumnAsScn(rs, "ENABLE_CHANGE#"))
                                .enabledTime(readTimestampAsInstant(rs, "ENABLE_TIME"))
                                .disabledScn(readScnColumnAsScn(rs, "DISABLE_CHANGE#"))
                                .disabledTime(readTimestampAsInstant(rs, "DISABLE_TIME"));
                        if (getOracleVersion().getMajor() >= 11) {
                            threadBuilder = threadBuilder.lastRedoSequenceNumber(rs.getLong("LAST_REDO_SEQUENCE#"))
                                    .lastRedoBlock(rs.getLong("LAST_REDO_BLOCK"))
                                    .lastRedoScn(readScnColumnAsScn(rs, "LAST_REDO_CHANGE#"))
                                    .lastRedoTime(readTimestampAsInstant(rs, "LAST_REDO_TIME"))
                                    .conId(rs.getLong("CON_ID"));
                        }
                        builder = threadBuilder.build();
                    }
                }
                return builder.build();
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to read the Oracle database redo thread state", e);
        }
    }

    public LogFile getArchiveLogFile(int threadId, long sequenceId) {
        try {
            return prepareQueryAndMap(
                    "SELECT NAME, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# FROM V$ARCHIVED_LOG WHERE THREAD#=? AND SEQUENCE#=?",
                    ps -> {
                        ps.setInt(1, threadId);
                        ps.setLong(2, sequenceId);
                    },
                    rs -> {
                        if (!rs.next()) {
                            return null;
                        }
                        return new LogFile(
                                rs.getString(1),
                                Scn.valueOf(rs.getString(2)),
                                Scn.valueOf(rs.getString(3)),
                                BigInteger.valueOf(rs.getLong(4)),
                                LogFile.Type.ARCHIVE,
                                threadId);
                    });
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to find archive log for redo thread {} and sequence {}", threadId, sequenceId, e);
            return null;
        }
    }

    /**
     * Get the table's Oracle {@code OBJECT_ID} from {@code ALL_OBJECTS}.
     *
     * @param tableId the table identifier, should not be {@code null}
     * @return the table's object id, or {@code null} if the table was not found
     * @throws SQLException if a database exception occurred
     */
    public Long getTableObjectId(TableId tableId) throws SQLException {
        return prepareQueryAndMap(
                "SELECT OBJECT_ID FROM ALL_OBJECTS WHERE OBJECT_TYPE='TABLE' AND OWNER=? AND OBJECT_NAME=?",
                ps -> {
                    ps.setString(1, tableId.schema());
                    ps.setString(2, tableId.table());
                }, rs -> rs.next() ? rs.getLong(1) : null);
    }

    /**
     * Get the table's Oracle {@code DATA_OBJECT_ID} from {@code ALL_OBJECTS}.
     *
     * @param tableId the table identifier, should not be {@code null}
     * @return the table's data object id, or {@code null} if the table was not found or has no data segment
     * @throws SQLException if a database exception occurred
     */
    public Long getTableDataObjectId(TableId tableId) throws SQLException {
        return prepareQueryAndMap(
                "SELECT DATA_OBJECT_ID FROM ALL_OBJECTS WHERE OBJECT_TYPE='TABLE' AND OWNER=? AND OBJECT_NAME=?",
                ps -> {
                    ps.setString(1, tableId.schema());
                    ps.setString(2, tableId.table());
                }, rs -> {
                    if (rs.next()) {
                        final long dataObjectId = rs.getLong(1);
                        return rs.wasNull() ? null : dataObjectId;
                    }
                    return null;
                });
    }

    /**
     * Resolves the owning table of the given Oracle object id from {@code ALL_OBJECTS}.
     *
     * Note that a dropped and purged object no longer exists in {@code ALL_OBJECTS} and cannot be
     * resolved by this method; such lookups return {@code null}.
     *
     * @param objectId the object id to resolve, should not be {@code null}
     * @param catalogName the catalog name to associate with the resolved identifier
     * @return the resolved table identifier, or {@code null} if the object id was not found
     * @throws SQLException if a database exception occurred
     */
    public TableId resolveTableIdByObjectId(Long objectId, String catalogName) throws SQLException {
        return prepareQueryAndMap(
                "SELECT OWNER, OBJECT_NAME FROM ALL_OBJECTS WHERE OBJECT_TYPE='TABLE' AND OBJECT_ID=?",
                ps -> ps.setLong(1, objectId),
                rs -> rs.next() ? new TableId(catalogName, rs.getString(1), rs.getString(2)) : null);
    }

    /**
     * Get the database character set used for {@code VARCHAR2}, {@code CHAR}, and {@code CLOB} data types.
     *
     * The database character set is set at database creation and does not change, so the result is
     * lazily fetched once per connection instance and cached. Backport of upstream dbz#1798
     * ({@code 77fb3d1ca}); fetched lazily so that connectors not using the hybrid mining strategy
     * never issue the query.
     *
     * @return the database character set
     */
    public synchronized CharacterSet getDatabaseCharacterSet() {
        if (databaseCharacterSet == null) {
            final String query = "SELECT NLS_CHARSET_ID(VALUE) FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET'";
            try {
                final Integer charsetId = queryAndMap(query, rs -> {
                    if (rs.next()) {
                        return rs.getInt(1);
                    }
                    return null;
                });
                if (charsetId == null || charsetId == 0) {
                    throw new SQLException("Failed to resolve Oracle's NLS_CHARACTERSET property");
                }
                databaseCharacterSet = CharacterSet.make(charsetId);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to resolve Oracle's NLS_CHARACTERSET property", e);
            }
        }
        return databaseCharacterSet;
    }

    /**
     * Get the nationalized character set used for {@code NVARCHAR} and {@code NCHAR} data types.
     *
     * The nationalized character set must be set at database creation, so the result is lazily
     * fetched once per connection instance and cached. Backport of upstream DBZ-3401
     * ({@code 648db8886}). It can only be {@code AL16UTF16} or {@code UTF8}.
     *
     * @return the national character set
     */
    public synchronized CharacterSet getNationalCharacterSet() {
        if (nationalCharacterSet == null) {
            final String query = "SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_NCHAR_CHARACTERSET'";
            try {
                final String nlsCharacterSet = queryAndMap(query, rs -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                });
                if ("AL16UTF16".equals(nlsCharacterSet)) {
                    nationalCharacterSet = CharacterSet.make(CharacterSet.AL16UTF16_CHARSET);
                }
                else if ("UTF8".equals(nlsCharacterSet)) {
                    nationalCharacterSet = CharacterSet.make(CharacterSet.UTF8_CHARSET);
                }
                else {
                    throw new SQLException("An unexpected NLS_NCHAR_CHARACTERSET detected: " + nlsCharacterSet);
                }
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to resolve Oracle's NLS_NCHAR_CHARACTERSET property", e);
            }
        }
        return nationalCharacterSet;
    }

    private static Scn readScnColumnAsScn(ResultSet rs, String columnName) throws SQLException {
        final String value = rs.getString(columnName);
        return Strings.isNullOrEmpty(value) ? Scn.NULL : Scn.valueOf(value);
    }

    private static Instant readTimestampAsInstant(ResultSet rs, String columnName) throws SQLException {
        final Timestamp value = rs.getTimestamp(columnName);
        return value == null ? null : value.toInstant();
    }
}
