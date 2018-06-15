package com.datatrees.datacenter.schema.service.repository;

import com.datatrees.datacenter.schema.service.loader.HistoryRecord;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDdlParser;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.*;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.text.ParsingException;
import io.debezium.util.AvroValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SchemaBuilder {

    private static final Logger logger = LoggerFactory.getLogger(SchemaBuilder.class);

    private TableSchemaBuilder tableSchemaBuilder;

    private DdlParser ddlParser = new MySqlDdlParser(false);

    private Tables latestTable = new Tables();

    private long skipped = 0;

    public SchemaBuilder(Configuration config) {
        AvroValidator schemaNameValidator = AvroValidator.create(logger);
        // Use MySQL-specific converters and schemas for values ...
        String timePrecisionModeStr = config.getString(MySqlConnectorConfig.TIME_PRECISION_MODE);
        TemporalPrecisionMode timePrecisionMode = TemporalPrecisionMode.parse(timePrecisionModeStr);
        String decimalHandlingModeStr = config.getString(MySqlConnectorConfig.DECIMAL_HANDLING_MODE);
        MySqlConnectorConfig.DecimalHandlingMode decimalHandlingMode = MySqlConnectorConfig.DecimalHandlingMode.parse(decimalHandlingModeStr);
        JdbcValueConverters.DecimalMode decimalMode = decimalHandlingMode.asDecimalMode();
        String bigIntUnsignedHandlingModeStr = config.getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode = MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode = bigIntUnsignedHandlingMode.asBigIntUnsignedMode();
        MySqlValueConverters valueConverters = new MySqlValueConverters(decimalMode, timePrecisionMode, bigIntUnsignedMode);
        this.tableSchemaBuilder = new TableSchemaBuilder(valueConverters, schemaNameValidator::validate);

    }

    public Map<TableId, TableSchema> buildSchema(HistoryRecord recovered) {

        String ddl = recovered.ddl();

        if (! isValidDdl(ddl)){
            logger.info(ddl);
            skipped ++;
            return null;
        }

        Map<TableId, TableSchema> id2Schema = new HashMap<>();
        if (ddl != null) {
            ddlParser.setCurrentSchema(recovered.databaseName()); // may be null

            try {
                logger.debug("Applying: {}", ddl);
                ddlParser.parse(ddl, latestTable);

                Set<TableId> changedTbl = latestTable.drainChanges();

                changedTbl.forEach(id -> {
                    Table table = latestTable.forTable(id);
                    TableSchema schema = tableSchemaBuilder.create("", table, null, null);
                    id2Schema.put(id, schema);
                });


            } catch (final ParsingException e) {
                logger.error("Cannot parse DDL statements {} stored in history, exiting", ddl);
                throw new RuntimeException(e);
            }

        }
        return id2Schema;
    }

    private boolean isValidDdl(String s){
        if (s.toUpperCase().startsWith("DROP TABLE")){
            return false;
        }
        if (s.toUpperCase().startsWith("SAVE ENDPOINT")){
            return false;
        }
        return true;
    }

}
