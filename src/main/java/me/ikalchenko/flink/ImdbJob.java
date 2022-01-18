package me.ikalchenko.flink;

import org.apache.flink.table.api.*;

public class ImdbJob {

	public static void main(String[] args) {
		final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());

		TableDescriptor titleBasicsDesc = TableDescriptor.forConnector("filesystem")
				.schema(Schema.newBuilder()
						.column("tconst", DataTypes.STRING())
						.column("titleType", DataTypes.STRING())
						.column("primaryTitle", DataTypes.STRING())
						.column("originalTitle", DataTypes.STRING())
						.column("isAdult", DataTypes.STRING())
						.column("startYear", DataTypes.INT())
						.column("endYear", DataTypes.INT())
						.column("runtimeMinutes", DataTypes.INT())
						.column("genres", DataTypes.ARRAY(DataTypes.STRING()))
						.build())
				.option("path", "/Users/ikalchenko/imdb_data/title.basics.tsv")
				.option("csv.field-delimiter", "\t")
				.option("csv.null-literal", "\\N")
				.option("csv.array-element-delimiter", ",")
				.option("csv.allow-comments", "true")
				.format("csv")
				.build();

		env.createTable("title_basics", titleBasicsDesc);

		Table titleBasics = env.from("title_basics");

		titleBasics.limit(20).execute().print();

	}
}
