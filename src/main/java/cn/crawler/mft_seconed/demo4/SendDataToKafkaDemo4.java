package cn.crawler.mft_seconed.demo4;

import cn.crawler.mft_seconed.KafkaEntity;
import cn.crawler.mft_seconed.demo2.SendDataToKafkaSql;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class SendDataToKafkaDemo4 {

    public static void main(String[] args){
        SendDataToKafkaDemo4 sendDataToKafkaDemo4 = new SendDataToKafkaDemo4();
        for(int i=0;i<40;i++){
            KafkaEntity build = KafkaEntity.builder().id(1).message("message"+i).create_time(System.currentTimeMillis()).name(""+1).build();
            System.out.println(build.toString());
            sendDataToKafkaDemo4.send("test-demo13", "123", JSON.toJSONString(build));
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
