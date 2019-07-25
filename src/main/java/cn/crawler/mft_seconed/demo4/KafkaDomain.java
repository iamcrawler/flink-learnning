package cn.crawler.mft_seconed.demo4;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaDomain {

    private String id;

    private Long times;

    private String message;

}
