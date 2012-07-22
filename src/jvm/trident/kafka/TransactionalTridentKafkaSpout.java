package trident.kafka;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.Map;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;
import trident.kafka.KafkaConfig.StaticHosts;


public class TransactionalTridentKafkaSpout implements IPartitionedTridentSpout<BatchMeta> {
    
    KafkaConfig _config;
    
    public TransactionalTridentKafkaSpout(KafkaConfig config) {
        _config = config;
    }
    
    class Coordinator implements IPartitionedTridentSpout.Coordinator {
        @Override
        public int numPartitions() {
            return computeNumPartitions();
        }

        @Override
        public void close() {
        }
    }
    
    class Emitter implements IPartitionedTridentSpout.Emitter<BatchMeta> {
        StaticPartitionConnections _connections;
        int partitionsPerHost;
        
        public Emitter() {
            _connections = new StaticPartitionConnections(_config);
            StaticHosts hosts = (StaticHosts) _config.hosts;
            partitionsPerHost = hosts.partitionsPerHost;            
        }
        
        @Override
        public BatchMeta emitPartitionBatchNew(TransactionAttempt attempt, TridentCollector collector, int partition, BatchMeta lastMeta) {
            SimpleConsumer consumer = _connections.getConsumer(partition);

            return KafkaUtils.emitPartitionBatchNew(_config, partition, consumer, attempt, collector, lastMeta);
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, int partition, BatchMeta meta) {
            SimpleConsumer consumer = _connections.getConsumer(partition);
                        
            ByteBufferMessageSet msgs = consumer.fetch(new FetchRequest(_config.topic, partition % partitionsPerHost, meta.offset, _config.fetchSizeBytes));
            long offset = meta.offset;
            for(MessageAndOffset msg: msgs) {
                if(offset == meta.nextOffset) break;
                if(offset > meta.nextOffset) {
                    throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                }
                KafkaUtils.emit(_config, attempt, collector, msg.message());
                offset = msg.offset();
            }            
        }
        
        @Override
        public void close() {
            _connections.close();
        }
    }
    

    @Override
    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator();
    }

    @Override
    public IPartitionedTridentSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter();
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }
    
    private int computeNumPartitions() {
        StaticHosts hosts =  _config.hosts;
        return hosts.hosts.size() * hosts.partitionsPerHost;      
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        backtype.storm.Config conf = new backtype.storm.Config();
        conf.registerSerialization(BatchMeta.class);
        return conf;
    }
}