package cn.crawler.mft_seconed.demo3;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by liuliang
 * on 2019/7/15
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class UserState implements Serializable {
    private Integer count;
}
