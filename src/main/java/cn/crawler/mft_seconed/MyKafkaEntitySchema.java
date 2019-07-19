package cn.crawler.mft_seconed;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class MyKafkaEntitySchema implements DeserializationSchema<KafkaEntity>, SerializationSchema<KafkaEntity> {
    @Override
    public KafkaEntity deserialize(byte[] message) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(KafkaEntity nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(KafkaEntity element) {
        return new byte[0];
    }

    @Override
    public TypeInformation<KafkaEntity> getProducedType() {
        return null;
    }
}
