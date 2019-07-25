package cn.crawler.mft_seconed.demo4;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;

import java.io.Serializable;
import java.lang.reflect.Field;


@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyEvent  {
    private String id;
    private Long eventTime;
    private String info;


}
