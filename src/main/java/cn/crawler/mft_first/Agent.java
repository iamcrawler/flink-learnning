package cn.crawler.mft_first;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by liuliang
 * on 2019/5/28
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Agent {

    private String id;

    private String parentId;

}
