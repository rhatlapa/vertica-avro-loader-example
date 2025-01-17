# vertica-avro-loader-example
Example for loading AVRO to Vertica

# Local Usage
## Prerequisites
Prerequisites necessary to run locally:
- `docker` running
- add `127.0.0.1 kafka` to `/etc/hosts`
- Maven
- Java 21

## Guide
Guide how to run `kafka` with schema registry locally:
- run `docker-compose up -d`, after that you can access `confluent` UI on address `localhost:9021`
- run `mvn clean package`
- create your config file (see config.json.template as template)
- make sure you've created the table in your Vertica DB, with configured columns of testName & testValue
- run `mvn exec:java -Dexec.mainClass=org.rh.example.vertica.FAvroParserExample -Dconfig.file=config.json`, where `config.json` is your file with the defined configs for connections to the DB
