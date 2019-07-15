package cn.crawler.mft_first;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by liuliang on 2019/5/16.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MallUserLogin implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;

    private String userId;

    private String address;

    private Date loginTime;

    private String email;

    private String ip;

    private Integer age;

}
