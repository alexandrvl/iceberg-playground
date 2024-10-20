package com.example;

import com.google.common.primitives.Ints;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;

import static org.apache.iceberg.FileFormat.PARQUET;

public class IcebergTransformationService {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    private final JdbcCatalog catalog;
    private final String namespace;
    private final String tableName;

    public IcebergTransformationService(JdbcCatalog catalog, String namespace, String tableName) {
        this.catalog = catalog;
        this.namespace = namespace;
        this.tableName = tableName;
    }

    public void transformToIceberg(InputStream inputStream) throws Exception {
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(tableIdentifier);
        var writer = getPartitionedFanoutWriter(table);
        writeToTable(inputStream, writer, table);
    }

    private static void writeToTable(InputStream inputStream, BaseTaskWriter<Record> writer, Table table) throws IOException {
        try (CSVParser csvParser = new CSVParser(new InputStreamReader(inputStream), CSVFormat.DEFAULT.withHeader())) {

            for (CSVRecord csvRecord : csvParser) {
                var schema = table.schema();
                var record = getGenericRecord(csvRecord, schema);
                writer.write(record);
            }

            writer.close();
            var dataFiles = writer.complete();

            Transaction transaction = table.newTransaction();
            for (DataFile file : dataFiles.dataFiles()) {
                transaction.newAppend().appendFile(file).commit();
            }
            transaction.commitTransaction();
        }
    }

    private static GenericRecord getGenericRecord(CSVRecord csvRecord, Schema schema) {
        var record = GenericRecord.create(schema);
        for (Types.NestedField field : schema.columns()) {
            String fieldName = field.name();
            if (field.name().equals("timestamp")) {
                LocalDateTime localDateTime = LocalDateTime.parse(csvRecord.get(fieldName), DATE_TIME_FORMATTER);
                record.setField(fieldName, localDateTime.atOffset(ZoneOffset.UTC));
            }
            if (field.name().equals("id")) {
                record.setField(fieldName, Integer.parseInt(csvRecord.get(fieldName)));
            }
            if (field.name().equals("name")) {
                record.setField(fieldName, csvRecord.get(fieldName));
            }
        }
        return record;
    }

    public void createTable() {
        Schema schema = new Schema(
                List.of(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "timestamp", Types.TimestampType.withZone())),
                Set.of(1)
        );
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                .day("timestamp")
                .build();
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
        catalog.createTable(tableIdentifier, schema, partitionSpec);
    }

    private static BaseTaskWriter<Record> getPartitionedFanoutWriter(Table table) {
        OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis()).defaultSpec(table.spec()).format(PARQUET).build();
        var partitionKey = new PartitionKey(table.spec(), table.spec().schema());
        var wrapper = new InternalRecordWrapper(table.spec().schema().asStruct());
        var appenderFactory = getTableAppender(table);
        return new PartitionedFanoutWriter<>(table.spec(), PARQUET, appenderFactory, outputFileFactory, table.io(), 1000000) {

            @Override
            protected PartitionKey partition(Record partitionKeyRecord) {
                partitionKey.partition(wrapper.wrap(partitionKeyRecord));
                return partitionKey;
            }
        };
    }

    public static GenericAppenderFactory getTableAppender(Table icebergTable) {
        return new GenericAppenderFactory(
                icebergTable.schema(),
                icebergTable.spec(),
                Ints.toArray(icebergTable.schema().identifierFieldIds()),
                icebergTable.schema(),
              null);
    }
}
