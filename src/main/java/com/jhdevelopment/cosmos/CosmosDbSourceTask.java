package com.jhdevelopment.cosmos;

import com.microsoft.azure.documentdb.*;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;


public class CosmosDbSourceTask extends SourceTask {

    private DocumentClient client;
    private String collectionLink;

    public String version() {
        return AppInfoParser.getVersion();
    }

    public void start(Map<String, String> map) {
        String endPointUri = map.get(CosmosDbSourceConfig.ENDPOINT_URI);
        String cosmosKey = map.get(CosmosDbSourceConfig.COSMOS_KEY);
        collectionLink = map.get(CosmosDbSourceConfig.COSMOS_COLLECTION_LINKS);


        this.client = new DocumentClient(
                endPointUri,
                cosmosKey,
                new ConnectionPolicy(),
                ConsistencyLevel.Session);
    }

    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        ChangeFeedOptions options = new ChangeFeedOptions();
        FeedResponse<Document> results = client.queryDocumentChangeFeed(collectionLink, options);
        for(Document activity : results.getQueryIterable()) {
            Date timeStamp = activity.getTimestamp();
            Map<String, String> sourcePartition = Collections.singletonMap("cosmosCollection", collectionLink);
            Map<String, String> sourceOffset = Collections.singletonMap(collectionLink, timeStamp.toString());
            Schema schema = null; //TODO get schema
            SourceRecord record = new SourceRecord(sourcePartition, sourceOffset,"test", schema, activity);
            records.add(record);        }

        return records;
    }

    public void stop() {
        this.client.close();
    }
}
