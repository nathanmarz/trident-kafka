package trident.kafka;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.Map;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;
import trident.kafka.KafkaConfig.StaticHosts;


public class OpaqueTridentKafkaSpout implements IOpaquePartitionedTridentSpout<BatchMeta> {
    public static final Logger LOG = Logger.getLogger(OpaqueTridentKafkaSpout.class);
    
    KafkaConfig _config;
    
    public OpaqueTridentKafkaSpout(KafkaConfig config) {
        _config = config;
    }
    
    @Override
    public IOpaquePartitionedTridentSpout.Emitter<BatchMeta> getEmitter(Map conf, TopologyContext context) {
        return new Emitter();
    }
    
    @Override
    public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map map, TopologyContext tc) {
        return new Coordinator();
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }    
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        backtype.storm.Config conf = new backtype.storm.Config();
        conf.registerSerialization(BatchMeta.class);
        return conf;
    }
    
    class Coordinator implements IOpaquePartitionedTridentSpout.Coordinator {
        @Override
        public void close() {
        }
    }
    
    class Emitter implements IOpaquePartitionedTridentSpout.Emitter<BatchMeta> {
        StaticPartitionConnections _connections;
        
        public Emitter() {
            _connections = new StaticPartitionConnections(_config);
        }

        @Override
        public BatchMeta emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, int partition, BatchMeta lastMeta) {
            try {
                SimpleConsumer consumer = _connections.getConsumer(partition);
                return KafkaUtils.emitPartitionBatchNew(_config, partition, consumer, attempt, collector, lastMeta);
            } catch(FailedFetchException e) {
                LOG.warn("Failed to fetch from partition " + partition);
                if(lastMeta==null) {
                    return null;
                } else {
                    BatchMeta ret = new BatchMeta();
                    ret.offset = lastMeta.nextOffset;
                    ret.nextOffset = lastMeta.nextOffset;
                    return ret;
                }
            }
        }

        @Override
        public int numPartitions() {
            StaticHosts hosts = (StaticHosts) _config.hosts;
            return hosts.hosts.size() * hosts.partitionsPerHost;
        }

        @Override
        public void close() {
            _connections.close();
        }        
    }    
}
