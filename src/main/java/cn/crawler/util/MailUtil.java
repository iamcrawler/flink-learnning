package cn.crawler.util;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.Random;

/**
 * Created by liuliang on 2019/5/18.
 */
public class MailUtil {


    public static String AuthMailSSL(String mail, String address) throws EmailException {
        //校验邮箱格式
        String format = "[\\w!#$%&'*+/=?^_`{|}~-]+(?:\\.[\\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\\w](?:[\\w-]*[\\w])?\\.)+[\\w](?:[\\w-]*[\\w])?";
        if (!mail.matches(format)) {
            throw new RuntimeException("邮箱格式不符合");
        }
        //生成随机数，发送邮件
        Random random = new Random();
        Integer i = random.nextInt(9999 - 1000 + 1) + 1000;
        HtmlEmail hemail = new HtmlEmail();
        hemail.setSSL(true);
        hemail.setHostName("smtp.163.com");
        //端口号
        hemail.setSmtpPort(465);
        hemail.setCharset(Charset.defaultCharset().name());
        hemail.addTo(mail);
        String[] arr = {"loqvliuliang@163.com"};
        hemail.addCc(arr);
        hemail.setFrom("loqvliuliang@163.com", "刘亮");
        hemail.setAuthentication("loqvliuliang@163.com", "handhand123");
        hemail.setSubject("【异地登陆提醒】");
        hemail.setMsg("警告：您的账号于" + DateUtil.format(new Date()) + "在【" + address + "】登陆");
        hemail.send();
        return i.toString();
    }
}
