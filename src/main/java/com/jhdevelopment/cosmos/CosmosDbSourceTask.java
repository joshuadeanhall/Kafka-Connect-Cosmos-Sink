package com.jhdevelopment.cosmos;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;


public class CosmosDbSourceTask extends SourceTask {

    private CosmosDbReader cosmosDbReader;

    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Starts the cosmos db reader and initializes the saved offsets
     * @param map
     */
    public void start(Map<String, String> map) {
        String endPointUri = map.get(CosmosDbSourceConfig.ENDPOINT_URI);
        String cosmosKey = map.get(CosmosDbSourceConfig.COSMOS_KEY);
        String collectionName = map.get(CosmosDbSourceConfig.COSMOS_COLLECTION_NAME);
        String databaseName = map.get(CosmosDbSourceConfig.COSMOS_DATABASE);
        int maxDocumentPerPartition = Integer.parseInt(map.get(CosmosDbSourceConfig.COSMOS_MAX_DOCUMENTS_PER_PARTITION));
        String x = 23;
        cosmosDbReader = new CosmosDbReader(endPointUri, cosmosKey, databaseName, collectionName, maxDocumentPerPartition);
        cosmosDbReader.start();
        //TODO improve the way offsets are loaded this would be easy to miss.
        loadOffsets();
    }

    /**
     *
     * @return a list of source recods
     * @throws InterruptedException
     */
    public List<SourceRecord> poll() throws InterruptedException {
        return cosmosDbReader.poll();
    }

    /**
     * Stops the task and the cosmosDbReader
     */
    public void stop() {
        cosmosDbReader.stop();
    }


    /**
     * Load the saved partition offsets so that it resumes at the last location for a partition
     */
    private void loadOffsets() {
        cosmosDbReader.loadOffsets(context.offsetStorageReader().offsets(cosmosDbReader.getPartitions()));
    }
}
