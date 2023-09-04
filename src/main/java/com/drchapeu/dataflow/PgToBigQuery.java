package com.drchapeu.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import java.sql.ResultSet;
import java.util.Base64;
import java.util.Arrays;

public class PgToBigQuery {

    // Interface para opções personalizadas
    public interface PgToBigQueryOptions extends PipelineOptions {
        @Description("JDBC connection URL")
        String getJdbcUrl();
        void setJdbcUrl(String jdbcUrl);

        @Description("BigQuery output table")
        @Default.String("indicium-sandbox:dataflow_testing.categories")
        String getOutputTable();
        void setOutputTable(String value);
    }

    public static void main(String[] args) {
        PgToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PgToBigQueryOptions.class);

        Pipeline p = Pipeline.create(options);

        // Definindo o esquema para BigQuery
        TableSchema schema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("category_id").setType("INTEGER"),
                new TableFieldSchema().setName("category_name").setType("STRING"),
                new TableFieldSchema().setName("description").setType("STRING"),
                new TableFieldSchema().setName("picture").setType("BYTES")
        ));

        // Ler dados do PostgreSQL
        p.apply("ReadFromPostgres", JdbcIO.<TableRow>read()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                "org.postgresql.Driver", options.getJdbcUrl()))
                        .withQuery("SELECT * FROM categories")
                        .withRowMapper(new JdbcIO.RowMapper<TableRow>() {
                            @Override
                            public TableRow mapRow(ResultSet resultSet) throws Exception {
                                TableRow row = new TableRow();
                                row.set("category_id", resultSet.getInt("category_id"));
                                row.set("category_name", resultSet.getString("category_name"));
                                row.set("description", resultSet.getString("description"));

                                // Convertendo a coluna picture para Base64
                                byte[] pictureBytes = resultSet.getBytes("picture");
                                if (pictureBytes != null) {
                                    String pictureBase64 = Base64.getEncoder().encodeToString(pictureBytes);
                                    row.set("picture", pictureBase64);
                                } else {
                                    row.set("picture", null);
                                }

                                return row;
                            }
                        }))
                // Escrever dados no BigQuery
                .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withSchema(schema)  // Usando o esquema definido acima
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                );

        p.run().waitUntilFinish();
    }
}
