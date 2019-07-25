package cn.crawler.mft_seconed.demo4;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaDemo4 {

    private String key;

    private Long timestamp;
}
