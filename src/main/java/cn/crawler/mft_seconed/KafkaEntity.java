package cn.crawler.mft_seconed;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by liuliang
 * on 2019/7/13
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class KafkaEntity {
    private String id;

    private String message;

}
