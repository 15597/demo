package com.example.demo;

import com.example.demo.config.jdbcConfig.HiveJdbcBaseDaoImpl;
import com.example.demo.config.jdbcConfig.MysqlMainJdbcBaseDaoImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@RunWith(SpringRunner.class)
@SpringBootTest(classes={DemoApplication.class})
public class test {

    @Autowired
    private HiveJdbcBaseDaoImpl hiveJdbcBaseDao;

    @Autowired
    private com.example.demo.config.jdbcConfig.MysqlMainJdbcBaseDaoImpl MysqlMainJdbcBaseDaoImpl;

    @Test
    public void testMysql() {
       /* String sql = "select * from student";
        List<Map<String, Object>> info = hiveJdbcBaseDaoImpl.getJdbcTemplate().queryForList(sql);
        System.out.println("hive中查出的数据是："+info);

        //校验该表是否在数据库中存在
        String sql1 = "select * from test";
        List<Map<String, Object>> info1 = MysqlMainJdbcBaseDaoImpl.getJdbcTemplate().queryForList(sql);
        System.out.println("mysql中查出的数据是："+info1);*/
        //hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition=false;");


        //开启允许所有分区都是动态的，否则必须要有静态分区才能使用
        //hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition.mode=nonstrict;");
        //hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition.mode=nonstrict");


    }

}

