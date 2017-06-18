package com.jhdevelopment.cosmos;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.errors.ConnectException;
import java.util.*;

public class CosmosDbSourceConnector extends SourceConnector {

    private String cosmosEndPointUri;
    private String cosmosKey;
    private String cosmosCollections;
    private String cosmosDatabaseName;
    private int maxDocumentPerPartition;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        cosmosEndPointUri = map.get(CosmosDbSourceConfig.ENDPOINT_URI);
        cosmosKey = map.get(CosmosDbSourceConfig.COSMOS_KEY);
        cosmosCollections = map.get(CosmosDbSourceConfig.COSMOS_COLLECTION_NAMES);
        cosmosDatabaseName = map.get(CosmosDbSourceConfig.COSMOS_DATABASE);
        maxDocumentPerPartition = Integer.parseInt(map.get(CosmosDbSourceConfig.COSMOS_MAX_DOCUMENTS_PER_PARTITION));


        if(cosmosEndPointUri == null || cosmosEndPointUri.isEmpty()) {
            throw new ConnectException("Missing EndPoint Uri");
        }

        if(cosmosKey == null || cosmosKey.isEmpty()) {
            throw new ConnectException("Missing Cosmos Key");
        }

        if(cosmosCollections == null || cosmosCollections.isEmpty()) {
            throw new ConnectException("Missing Cosmos Collections");
        }

        if(cosmosDatabaseName == null || cosmosDatabaseName.isEmpty()) {
            throw new ConnectException("Missing Cosmos DatabaseName");
        }

    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDbSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        String[] collections = cosmosCollections.split(",");
        int configsToCreate = Math.min(maxTasks, collections.length);
        for(int i=0; i<configsToCreate; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(CosmosDbSourceConfig.ENDPOINT_URI, cosmosEndPointUri);
            config.put(CosmosDbSourceConfig.COSMOS_KEY, cosmosKey);
            config.put(CosmosDbSourceConfig.COSMOS_COLLECTION_NAME, collections[i]);
            config.put(CosmosDbSourceConfig.COSMOS_DATABASE, cosmosDatabaseName);
            config.put(CosmosDbSourceConfig.COSMOS_MAX_DOCUMENTS_PER_PARTITION, Integer.toString(maxDocumentPerPartition));
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CosmosDbSourceConfig.config;
    }
}
