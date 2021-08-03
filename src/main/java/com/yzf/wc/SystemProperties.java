package com.yzf.wc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @author ：zhangyu
 * @date ：Created in 2021/7/16 10:16
 * @description：
 */
public class SystemProperties {
    public static void main(String[] args) throws IOException {
        // 基于ClassLoder读取配置文件
        Properties prop = new Properties();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(""));
//        prop.load(SystemProperties.class.getClassLoader().getResourceAsStream(""));
        prop.load(bufferedReader);
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        String dt = prop.getProperty("dt");

        System.out.println("username:" + username);
        System.out.println("password:" + password);
        System.out.println("dt:" + dt);

    }
}
