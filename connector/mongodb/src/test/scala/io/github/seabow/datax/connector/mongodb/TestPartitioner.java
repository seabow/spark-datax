package io.github.seabow.datax.connector.mongodb;

import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;
import com.mongodb.spark.sql.connector.read.partitioner.Partitioner;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class TestPartitioner implements Partitioner {
    @Override
    public List<MongoInputPartition> generatePartitions(ReadConfig readConfig) {
        return singletonList(
                new MongoInputPartition(
                        0,
                        readConfig.getAggregationPipeline(),
                        readConfig
                                .withClient(
                                        c -> {
                                            MongoDatabase db = c.getDatabase(readConfig.getDatabaseName());
                                            return c.getClusterDescription();
                                        })
                                .getServerDescriptions()
                                .stream()
                                .flatMap(sd -> sd.getHosts().stream())
                                .distinct()
                                .collect(Collectors.toList())));
    }
}
