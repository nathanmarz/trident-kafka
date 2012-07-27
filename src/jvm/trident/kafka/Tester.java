package trident.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.Arrays;
import java.util.List;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;


public class Tester {
    public static void main(String[] args) throws Exception {
        TridentTopology topology = new TridentTopology();
        List<String> hosts = Arrays.asList("localhost:9092");
        KafkaConfig kafkaConf = new KafkaConfig(KafkaConfig.StaticHosts.fromHostString(hosts, 3), "test");
        kafkaConf.scheme = new StringScheme();
        topology.newStream("mykafka", new TransactionalTridentKafkaSpout(kafkaConf))
//                .aggregate(new Count(), new Fields("count"))
                .each(new Fields("str"), new Debug());
        
        LocalCluster cluster = new LocalCluster();
        
        StormTopology topo = topology.build();
        
        cluster.submitTopology("kafkatest", new Config(), topo);
        KillOptions killopts = new KillOptions();
        killopts.set_wait_secs(0);
        Utils.sleep(5000);
        cluster.killTopologyWithOpts("kafkatest", killopts);
        Utils.sleep(5000);

        cluster.submitTopology("kafkatest", new Config(), topo);        
        Utils.sleep(60000);
    }
}
