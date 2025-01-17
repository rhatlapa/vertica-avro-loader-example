package org.rh.example.vertica;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.rh.example.Config;
import org.rh.example.avro.TestDataAvro;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FAvroParserExample {
	private static final String TOPIC = "test-topic";

	public static void main(String[] args) throws SQLException, IOException {
		var objectMapper = new ObjectMapper()
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

		var configFile = new File(System.getProperty("config.file", "config.json"));
		var config = objectMapper.readValue(configFile, Config.class);

		var dbProps = new Properties();
		dbProps.put("user", config.db().username());
		dbProps.put("password", config.db().password());

		var copyQuery = String.format("COPY %s FROM STDIN PARSER FAVROPARSER(flatten_arrays=false,flatten_maps=TRUE,flatten_records=TRUE) REJECTED DATA AS TABLE %s",
					config.db().table(), config.db().table() + "_rejected");

		try (var connection = DriverManager.getConnection(config.db().connectionString(), dbProps);
				var schemaRegistryClient = new CachedSchemaRegistryClient(config.schemaRegistry().schemaRegistryUrl(), 10)) {

			var dataSerializer = new KafkaAvroSerializer(schemaRegistryClient);
			dataSerializer.configure(Map.of("schema.registry.url", config.schemaRegistry().schemaRegistryUrl()), false);
			var firstMessage = dataSerializer.serialize(TOPIC, new TestDataAvro("name1", "value1"));
			var secondMessage = dataSerializer.serialize(TOPIC, new TestDataAvro("name2", null));
			var records = List.of(firstMessage, secondMessage);

			try {
				VerticaCopyStream vstream = new VerticaCopyStream((VerticaConnection) connection, copyQuery);
				vstream.start();

				for (var valueAsBytes : records) {
					if (valueAsBytes.length < 5) {
						throw new RuntimeException("Missing schema bytes");
					}
					var schemaId = Ints.fromByteArray(ArrayUtils.subarray(valueAsBytes, 1, 5));
					log.info("Schema ID: {}", schemaId);

					var avroSchema = (AvroSchema) schemaRegistryClient.getSchemaById(schemaId);
					var schema = avroSchema.rawSchema();
					GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
					DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
					try (var bout = new ByteArrayOutputStream()) {
						dataFileWriter.create(schema, bout);
						dataFileWriter.appendEncoded(ByteBuffer.wrap(Arrays.copyOf(valueAsBytes, valueAsBytes.length)));
						dataFileWriter.close();
						try (var is = new ByteArrayInputStream(bout.toByteArray())) {
							vstream.addStream(is);
							log.info("Before execute - Rejects {}", vstream.getRejects());
							log.info("Before execute - Row count {}", vstream.getRowCount());
							vstream.execute();
						}
					}
				}
				log.info("Rejects {}", vstream.getRejects());
				log.info("Row count {}", vstream.getRowCount());

				var rowCount = vstream.finish();
				log.info("After finish - Rejects {}", vstream.getRejects());
				log.info("After finish - Row count {}", vstream.getRowCount());
				log.info("After finish - Row count return {}", rowCount);

			} catch (IOException | RestClientException | SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}



}
