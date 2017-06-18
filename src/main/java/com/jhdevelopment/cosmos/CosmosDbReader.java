package com.jhdevelopment.cosmos;

import com.jhdevelopment.cosmos.utils.SchemaHelper;
import com.microsoft.azure.documentdb.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.*;

public class CosmosDbReader {
    private static int defaultMaxRecords = 100;
    private DocumentClient client;
    private String endPointUri;
    private String cosmosKey;
    private String collectionLink;
    private String databaseName;
    private String collectionName;
    private int maxRecords;
    private Map<String, String> partitionOffsets;
    private Schema schema;
    private SchemaHelper schemaHelper;

    public CosmosDbReader(String endPointUri, String cosmosKey, String databaseName, String collectionName, int maxRecords) {
        this.endPointUri = endPointUri;
        this.cosmosKey = cosmosKey;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.maxRecords = maxRecords;

        partitionOffsets = new HashMap<>();
        schemaHelper = new SchemaHelper();

        collectionLink = String.format("dbs/%1$s/colls/%2$s", databaseName, collectionName);
    }

    public CosmosDbReader(String endPointUri, String cosmosKey, String databaseName, String collectionName) {
        this(endPointUri, cosmosKey, databaseName, collectionName, defaultMaxRecords);
    }

    /**
     * Starts the CosmosDb Client and builds the partition dictionary
     */
    public void start() {
        this.client = new DocumentClient(
                endPointUri,
                cosmosKey,
                new ConnectionPolicy(),
                ConsistencyLevel.Session);

        //TODO this only runs at startup but it may be possible for new partitions to appear.  Need a way to refresh the partition list
        buildPartitionDictionary();

        schema = SchemaBuilder
                .struct()
                .name("schema".concat("_").concat(collectionName))
                .field("database", Schema.OPTIONAL_STRING_SCHEMA)
                .field("collection", Schema.OPTIONAL_STRING_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
                .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
                .field("etag", Schema.OPTIONAL_STRING_SCHEMA)
                .field("object", Schema.OPTIONAL_STRING_SCHEMA);
    }

    /**
     * Apply saved offsets to current partitions.
     * @param offsets initial partition offsets
     */
    public void loadOffsets(Map<Map<String, String>, Map<String, Object>> offsets) {
        for (Map.Entry<String, String> partitionKeyOffset : partitionOffsets.entrySet()) {
            Map<String, Object> savePartitionOffset = offsets.get(Collections.singletonMap(getPartitionName(), partitionKeyOffset.getKey()));
            if(savePartitionOffset == null || savePartitionOffset.isEmpty()) {
                continue;
            }
            String offset = (String)savePartitionOffset.get(partitionKeyOffset.getKey());
            partitionKeyOffset.setValue(offset);
        }
    }

    //TODO run partitions in parallel to improve performance.
    /**
     * Get the next available records
     * @return a list of source records
     */
    public List<SourceRecord> poll() {
        ArrayList<SourceRecord> records = new ArrayList<>();

        for (Map.Entry<String, String> partitionKeyOffset : partitionOffsets.entrySet()) {
            ChangeFeedOptions options = new ChangeFeedOptions();
            options.setStartFromBeginning(true); //TODO this should be a parameter
            options.setPageSize(maxRecords);
            options.setPartitionKeyRangeId(partitionKeyOffset.getKey());
            options.setRequestContinuation(partitionKeyOffset.getValue());

            FeedResponse<Document> response = client.queryDocumentChangeFeed(collectionLink, options);

            String partitionName = getPartitionName();
            for(Document result : response.getQueryIterable()) {
                SourceRecord record = new SourceRecord(Collections.singletonMap(partitionName, partitionKeyOffset.getKey()),
                        Collections.singletonMap(partitionKeyOffset.getKey(), result.getInt("_lsn").toString()), partitionName, schema, schemaHelper.toStruct(result, schema, databaseName, collectionName));

                records.add(record);
            }

            partitionKeyOffset.setValue(response.getResponseContinuation());
        }

        return records;
    }

    /**
     * Get the partitions that the collection has.
     * @return List of partitions
     */
    public List<Map<String, String>> getPartitions() {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (Map.Entry<String, String> partitionKeyOffset : partitionOffsets.entrySet()) {
            String partitionName = getPartitionName();
            Map<String, String> partition = Collections.singletonMap(partitionName, partitionKeyOffset.getKey());

            partitions.add(partition);
        }

        return partitions;
    }

    /**
     * Builds a list of the partitions on the collection
     */
    private void buildPartitionDictionary() {
        String continuation = null;
        do {

            FeedOptions options = new FeedOptions();
            options.setRequestContinuation(continuation);
            FeedResponse<PartitionKeyRange> feedResponse = this.client.readPartitionKeyRanges(collectionLink, options);

            for (PartitionKeyRange partitionKeyRange : feedResponse.getQueryIterable()) {
                partitionOffsets.put(partitionKeyRange.getId(), "0");
            }
            continuation = feedResponse.getResponseContinuation();
        } while(continuation != null);
    }


    /**
     * Get the name of the collection partition
     * @return partition name
     */
    private String getPartitionName() {
        return String.format("%1$s_%2$s", databaseName, collectionName);
    }


    /**
     * Stop the task and the client
     */
    public void stop() {
        client.close();
    }


}
