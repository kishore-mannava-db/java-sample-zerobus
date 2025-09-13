package org.example;

import com.databricks.zerobus_sdk.*;
import com.databricks.zerobus.*;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class Main {
   private static final String TABLE_NAME = "unity.catalog.air_quality";
    private static final String ZEROBUS_ENDPOINT = "<WORKSPACE_ID>.ingest.cloud.databricks.com";
    private static final String UNITY_CATALOG_ENDPOINT = "<YOUR_DATABRICKS_WORKSPACE_URL>";
    private static final String UC_TOKEN = "<YOUR_PAT>";



    private static final int NUM_RECORDS = 100_000;

    public static void main(String[] args) {
        try {
            // Callback that will be called when an acknowledgment is received from the server.
            Consumer<IngestRecordResponse> ackCallback = response ->
                    System.out.println("Record ack for offset: " + response.getDurabilityAckUpToOffset());

            // Table properties including table name and record schema class.
            TableProperties<TestTableeOuterClass.TestTablee> tableProps =
                    new TableProperties<>(TABLE_NAME, TestTableeOuterClass.TestTablee.class);

            // Stream configuration including max in-flight records and callback.
            StreamConfigurationOptions options = StreamConfigurationOptions.builder()
                    .setMaxInflightRecords(50_000)
                    .setAckCallback(ackCallback)
                    .build();

            // Create the Zerobus SDK handle.
            ZerobusSdk zerobusSdk =
                    new ZerobusSdk(ZEROBUS_ENDPOINT, UNITY_CATALOG_ENDPOINT, UC_TOKEN);

            // Create the Zerobus stream.
            ZerobusStream<TestTableeOuterClass.TestTablee> stream =
                    zerobusSdk.createStream(tableProps, options);

            System.out.println("Stream created: " + stream.getStreamId());

            // Prepare and ingest records.
            System.out.println("Ingesting " + NUM_RECORDS + " records ...");

            ArrayList<CompletableFuture<?>> futures = new ArrayList<>(NUM_RECORDS);

            // Build a sample record (reused for all inserts).
            TestTableeOuterClass.TestTablee row = TestTableeOuterClass.TestTablee.newBuilder()
                    .setId(1)
                    .setName("john")
                    .setSubmitCounter(1)
                    .build();

            long startTime = System.currentTimeMillis();

            for (int i = 0; i < NUM_RECORDS; i++) {
                IngestRecordResult result = stream.ingestRecord(row);

// Blocking on the future that the SDK accepted the record.
                result.getRecordAccepted().get();

// Not blocking on the future that the record was made durable.
                futures.add(result.getWriteCompleted());

                if ((i + 1) % 10_000 == 0) {
                    System.out.println("Ingested " + (i + 1) + " records");
                }
            }

            // Wait for all records to be acknowledged.
            // NOTE: It is possible that callbacks are called after flush finishes.
            System.out.println("Flushing records ...");
            stream.flush();

            long endTime = System.currentTimeMillis();
            System.out.println("Flush completed, ingested " + NUM_RECORDS +
                    " records in " + (endTime - startTime) + "ms");

            // Wait for all ingestion futures to complete.
            for (CompletableFuture<?> future : futures) {
                future.get();
            }

            // Close the stream.
            stream.close();
            System.out.println("Stream closed successfully.");
            System.out.println("Records per second: " +
                    (NUM_RECORDS * 1000.0 / (endTime - startTime)));

        } catch (ZerobusException e) {
            System.err.println("Error creating stream: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
        }
    }
}
