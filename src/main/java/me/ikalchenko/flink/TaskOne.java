package me.ikalchenko.flink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;

import java.nio.file.Paths;

import static org.apache.flink.table.api.Expressions.$;

public class TaskOne {

    private void readData() {
        String titleBasicsPath = Paths.get(ImdbJob.filePrefix, "title.basics.tsv").toString();
        Schema titleBasicsSchema = Schema.newBuilder()
                .column("tconst", DataTypes.STRING())
                .column("titleType", DataTypes.STRING())
                .column("primaryTitle", DataTypes.STRING())
                .column("originalTitle", DataTypes.STRING())
                .column("isAdult", DataTypes.STRING())
                .column("startYear", DataTypes.INT())
                .column("endYear", DataTypes.INT())
                .column("runtimeMinutes", DataTypes.INT())
                .column("genres", DataTypes.ARRAY(DataTypes.STRING()))
                .build();

        TableDescriptor titleBasicsDesc = CsvTableReader.read(titleBasicsSchema, titleBasicsPath);

        ImdbJob.env.createTable("title_basics", titleBasicsDesc);

    }

    public void getResult() {
        readData();

        Table titleBasics = ImdbJob.env.from("title_basics");

        titleBasics.limit(20).execute().print();
    }
}
