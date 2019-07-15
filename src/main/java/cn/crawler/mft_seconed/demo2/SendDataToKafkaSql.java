package cn.crawler.mft_seconed.demo2;

import cn.crawler.mft_seconed.KafkaEntity;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by liuliang
 * on 2019/7/13
 */
public class SendDataToKafkaSql {
    public static void main(String[] args){
        SendDataToKafkaSql sendDataToKafka = new SendDataToKafkaSql();
        for(int i=100;i<200;i++){
            KafkaEntity build = KafkaEntity.builder().message("meaasge" + i).id(UUID.randomUUID() + "").build();
            sendDataToKafka.send("test_sql", "123", JSON.toJSONString(build));
        }

    }

    public void send(String topic,String key,String data){
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.19.141.60:31090");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
        for(int i=1;i<2;i++){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(new ProducerRecord<String, String>(topic, key+i, data));
        }
        producer.close();
    }
}
