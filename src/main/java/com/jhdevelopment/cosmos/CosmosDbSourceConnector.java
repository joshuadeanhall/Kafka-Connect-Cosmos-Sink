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
    private String cosmosCollection;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        cosmosEndPointUri = map.get(CosmosDbSourceConfig.ENDPOINT_URI);
        cosmosKey = map.get(CosmosDbSourceConfig.COSMOS_KEY);
        cosmosCollection = map.get(CosmosDbSourceConfig.COSMOS_COLLECTION_LINKS);

        if(cosmosEndPointUri == null || cosmosEndPointUri.isEmpty()) {
            throw new ConnectException("Missing EndPoint Uri");
        }

        if(cosmosKey == null || cosmosKey.isEmpty()) {
            throw new ConnectException("Missing Cosmos Key");
        }

        if(cosmosCollection == null || cosmosCollection.isEmpty()) {
            throw new ConnectException("Missing Cosmos Collections");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDbSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        //TODO handle multiple collections

        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(CosmosDbSourceConfig.ENDPOINT_URI, cosmosEndPointUri);
        config.put(CosmosDbSourceConfig.COSMOS_KEY, cosmosKey);
        config.put(CosmosDbSourceConfig.COSMOS_COLLECTION_LINKS, cosmosCollection);
        configs.add(config);

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
