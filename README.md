# Java Sample Zerobus

This project demonstrates how to use the Databricks Zerobus SDK to ingest records into a Unity Catalog table using Java.

## Prerequisites
- Java 17 or higher
- Maven
- Databricks Unity Catalog workspace
- Personal Access Token (PAT) for Databricks

## Project Structure
- `src/main/java/org/example/Main.java`: Main application logic for ingesting records.
- `src/main/java/org/example/TestTableeOuterClass.java`: Generated class from the protobuf definition.
- `test_tablee.proto`: Protobuf schema for the table records.
- `jar/databricks_zerobus-0.0.6_fat.jar`: Zerobus SDK jar (fat jar).
- `scripts/`: Scripts for generating protobuf classes.
- `pom.xml`: Maven configuration.

## Setup
1. **Clone the repository**
2. **Generate Java classes from proto**:
   - Run `scripts/build_proto.sh` or use `generate_proto.py` to generate `TestTableeOuterClass.java` from `test_tablee.proto`.
3. **Configure credentials**:
   - Update `Main.java` with your Databricks workspace ID, workspace URL, and PAT.
4. **Build the project**:
   - Run `mvn clean package`

## Running the Application
```
mvn exec:java -Dexec.mainClass="org.example.Main"
```

## Notes
- The Zerobus SDK jar is referenced in `pom.xml` using the `system` scope. Ensure the jar exists at `jar/databricks_zerobus-0.0.6_fat.jar`.
- The application ingests 100,000 records into the specified Unity Catalog table.

## License
This project is for demonstration purposes only.

