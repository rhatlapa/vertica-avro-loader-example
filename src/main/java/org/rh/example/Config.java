package org.rh.example;

public record Config(SchemaRegistryConfig schemaRegistry,
					 DBConfig db) {

	public record SchemaRegistryConfig(String schemaRegistryUrl) {

	}

	public record DBConfig(String connectionString, String username, String password, String table) {

	}
}
