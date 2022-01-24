package me.ikalchenko.flink;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class CsvTableReader {
    private CsvTableReader() {}

    public static TableDescriptor read(Schema schema, String path) {
        return TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", path)
                .option("csv.field-delimiter", "\t")
                .option("csv.null-literal", "\\N")
                .option("csv.array-element-delimiter", ",")
                .option("csv.allow-comments", "true")
                .format("csv")
                .build();
    }
}
