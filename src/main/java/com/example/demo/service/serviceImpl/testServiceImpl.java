package com.example.demo.service.serviceImpl;

import com.example.demo.config.jdbcConfig.HiveJdbcBaseDaoImpl;
import com.example.demo.mapper.testMapper;
import com.example.demo.service.testService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.FilenameFilter;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;

@Service
public class testServiceImpl implements testService {
    @Autowired
    private com.example.demo.mapper.testMapper testMapper;

    private static String fileName;

    private static String DBName;

    private static Logger log = LoggerFactory.getLogger(testServiceImpl.class);

    @Value("${files.name}")
    public void setFileName(String file_Name) {
        fileName = file_Name;
    }

    @Value("${spring.datasource.hive.DBName}")
    public void setDBName(String DBName) {
        testServiceImpl.DBName = DBName;
    }

    @Autowired
    private HiveJdbcBaseDaoImpl hiveJdbcBaseDao;


    @Override
    public boolean moveDateToLocal(String sqlStr) {
        List<LinkedHashMap<String, Object>> mapList = null;
        try {
            mapList = testMapper.getDates(sqlStr);
            if (mapList != null) {
                for (LinkedHashMap<String, Object> map : mapList) {
                    String update_sql = (String) map.get("UPDATE_SQL");
                    String table_Name = (String) map.get("TABLE_NAME");
                    String insert_sql = "insert overwrite table " + table_Name + " " + update_sql;
                    moveDate(insert_sql, table_Name);
                }
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("导出数据到本地出现的异常为:"+e.toString());
        }
        return false;

    }

    @Override
    @Transactional
    public String moveToHive(String FileName, Integer loadType) {
        String tableName = findTableByFileName(FileName);
        if(tableName==null){
            return "文件名格式有错误";
        }
        //判断表是否存在
        boolean flag = validateTableNameExistByHive(tableName);
        List<LinkedHashMap<String, Object>> configList = testMapper.getConfigList(tableName);
        String filePath = (String) configList.get(0).get("filePath");
        Integer loadFormat = (Integer) configList.get(0).get("loadFormat");
        List<LinkedHashMap<String, Object>> partitionsList = testMapper.getPartitionsList(tableName);
        List<LinkedHashMap<String, Object>> indexList = testMapper.getIndexList(tableName);
        if (flag) {
            log.info("表已存在,开始导入数据");
            //表示已存在,导入数据即可
            String info = moveToHiveUtil(loadType, loadFormat, filePath, FileName, tableName, partitionsList, indexList);
            return info;
        } else {
            log.info("表不存在,开始创建表");
            //不存在,创建表后导入数据
            List<LinkedHashMap<String, Object>> filedList = testMapper.getFiledList(tableName);
            try {
                StringBuilder sb = new StringBuilder();
                StringBuilder sb1 = new StringBuilder();
                sb.append("create table if not exists " + tableName + "(");
                sb1.append("create table if not exists " + tableName +"_tmp"+ "(");
                for (int i = 0; i < filedList.size(); i++) {
                    Object filedName = filedList.get(i).get("filedName");
                    Object filedType = filedList.get(i).get("filedType");
                    if (i < filedList.size() - 1) {
                        sb.append(filedName + " " + filedType + ",");
                        sb1.append(filedName + " " + filedType + ",");
                    } else {
                        sb.append(filedName + " " + filedType + ")");
                        sb1.append(filedName + " " + filedType);
                    }
                }
                if (partitionsList != null) {
                    sb1.append(",");
                    sb.append(" partitioned by(");
                    for (int i = 0; i < partitionsList.size(); i++) {
                        String partitionName = (String) partitionsList.get(i).get("partitionName");
                        String partitionType = (String) partitionsList.get(i).get("partitionType");
                        if (i < partitionsList.size() - 1) {
                            sb.append(partitionName + " " + partitionType + ",");
                            sb1.append(partitionName + " " + partitionType + ",");
                        } else {
                            sb.append(partitionName + " " + partitionType + ")");
                            sb1.append(partitionName + " " + partitionType + ")");
                        }
                    }
                    sb.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'");
                    sb1.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'");
                    //创建正式表
                    hiveJdbcBaseDao.getJdbcTemplate().execute(sb.toString());
                    //创建临时表
                    hiveJdbcBaseDao.getJdbcTemplate().execute(sb1.toString());
                }
            } catch (Exception e) {
                log.error("创建表失败的异常为:" + e);
                e.printStackTrace();
            }
            try {
                if (indexList != null) {
                    for (LinkedHashMap<String, Object> map : indexList) {
                        Object index_name = map.get("index_Name");
                        Object filed_Name = map.get("filed_Name");
                        String createIndex_sql = "create index " + index_name + " on table " + tableName + "(" + filed_Name + ")" +
                                " as 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' with deferred rebuild IN TABLE " + tableName +"_"+filed_Name+ "_index";
                        //创建索引
                        hiveJdbcBaseDao.getJdbcTemplate().execute(createIndex_sql);
                        }
                    log.info("创建索引成功");
                }
            } catch (DataAccessException e) {
                e.printStackTrace();
                log.error("创建索引失败的异常为:"+e);
                return "创建索引失败";
            }
            String info = moveToHiveUtil(loadType, loadFormat, filePath, FileName, tableName, partitionsList, indexList);
            return info;
        }
    }


    public void moveDate(String insert_sql, String tableName) throws Exception {
        hiveJdbcBaseDao.getJdbcTemplate().execute("create table if not exists " + tableName + "(id string,value1 string,value2 string,value3 string)" + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'");
        hiveJdbcBaseDao.getJdbcTemplate().execute(insert_sql);
        String order = "insert overwrite local directory '"
                + fileName + tableName + "'row format delimited fields terminated by ','"
                + "  select * from " + tableName;
        hiveJdbcBaseDao.getJdbcTemplate().execute(order);
    }

    //通过文件名找到数据库中相对应的表名
    public String findTableByFileName(String FileName){
        List<String> fileNameList = testMapper.getFileNameList();
        boolean flag =false;
        StringBuilder sb = null;
        for (String s: fileNameList) {
            for (String pattern : s.split("_")) {
                if(!pattern.equals("*")){
                    sb =new StringBuilder();
                    flag = Pattern.matches(".*"+pattern+".*",FileName);
                    sb.append(String.valueOf(flag));
                }
            }
            if(!sb.toString().contains("false")){
                return testMapper.getTableName(s);
            }
        }
        return null;

    }

    public boolean validateTableNameExistByHive(String tableName) {
        //查询库中的所有表
        String sql = " show tables in " + DBName;
        //返回 List<Map<String, Object>>
        List<Map<String, Object>> count = hiveJdbcBaseDao.getJdbcTemplate().queryForList(sql);
        for (Map<String, Object> map : count) {
            for (String s : map.keySet()) {
                //遍历校验是否有一致的表
                if (tableName.equals(map.get(s))) {
                    return true;
                }
            }
        }
        return false;
    }

    //将数据存入到hive中
    public String moveToHiveUtil(Integer loadType, Integer loadFormat, String filePath, String FileName, String tableName, List<LinkedHashMap<String, Object>> partitionsList, List<LinkedHashMap<String, Object>> indexList) {
        //存在,载入数据即可
        try {

            StringBuilder sb =new StringBuilder();
            for (int i = 0; i < partitionsList.size(); i++) {
                if (i < partitionsList.size() - 1) {
                    sb.append(partitionsList.get(i).get("partitionName")+",");
                } else {
                    sb.append(partitionsList.get(i).get("partitionName"));
                }
            }

            if (loadType == 0) {
                //表示增量添加数据
                if (loadFormat == 0) {
                    //表示从服务器上将数据存到hive的临时表
                    String move = "load data local inpath " + "'" + filePath + FileName + "'" + " overwrite into table " + tableName + "_tmp";
                    hiveJdbcBaseDao.getJdbcTemplate().execute(move);
                    //开启动态分区 默认时false
                    hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition=true");
                    //开启允许所有分区都是动态的，否则必须要有静态分区才能使用
                    hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition.mode=nonstrict");
                    //通过insert语句将临时表的数据存入最终表,只有insert语句才能实现动态分区
                    hiveJdbcBaseDao.getJdbcTemplate().execute("insert into table "+tableName+" partition"+"("+sb.toString()+") "
                    +"select * from "+tableName+"_tmp");
                } else if (loadFormat == 1) {
                    //表示从服务器上将数据存到hive的临时表
                    String move = "load data inpath " + "'" + filePath + FileName + "'" + " overwrite into table " + tableName + "_tmp";
                    hiveJdbcBaseDao.getJdbcTemplate().execute(move);
                    //开启动态分区 默认时false
                    hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition=true");
                    //开启允许所有分区都是动态的，否则必须要有静态分区才能使用
                    hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition.mode=nonstrict");
                    hiveJdbcBaseDao.getJdbcTemplate().execute("insert into table "+tableName+" partition"+"("+sb.toString()+") "
                            +"select * from "+tableName+"_tmp");
                } else {
                    return "请填写正确的加载形式.loadFormat只能为0或1";
                }
            } else if (loadType == 1) {
                //表示全量添加数据
                if (loadFormat == 0) {
                    //表示从服务器上将数据存到hive的临时表
                    String move = "load data local inpath " + "'" + filePath + FileName + "'" + " overwrite into table " + tableName + "_tmp";
                    hiveJdbcBaseDao.getJdbcTemplate().execute(move);
                    //开启动态分区 默认时false
                    hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition=true");
                    //开启允许所有分区都是动态的，否则必须要有静态分区才能使用
                    hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition.mode=nonstrict");
                    hiveJdbcBaseDao.getJdbcTemplate().execute("insert  overwrite table "+tableName+" partition"+"("+sb.toString()+") "
                            +"select * from "+tableName+"_tmp");
                } else if (loadFormat == 1) {
                    //表示从服务器上将数据存到hive的临时表
                    String move = "load data inpath " + "'" + filePath + FileName + "'" + " overwrite into table " + tableName + "_tmp";
                    hiveJdbcBaseDao.getJdbcTemplate().execute(move);
                    //开启动态分区 默认时false
                    hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition=true");
                    //开启允许所有分区都是动态的，否则必须要有静态分区才能使用
                    hiveJdbcBaseDao.getJdbcTemplate().execute("set hive.exec.dynamic.partition.mode=nonstrict");

                    hiveJdbcBaseDao.getJdbcTemplate().execute("insert  overwrite table "+tableName+" partition"+"("+sb.toString()+") "
                            +"select * from "+tableName+"_tmp");
                } else {
                    return "请填写正确的加载形式.loadFormat只能为0或1";
                }
            } else {
                return "请填写正确的加载类型.loadType只能为0或1";
            }
            //重建索引值
            if (indexList != null) {
                for (LinkedHashMap<String, Object> map : indexList) {
                    Object index_name = map.get("index_Name");
                    hiveJdbcBaseDao.getJdbcTemplate().execute("alter index " + index_name + " on " + tableName + " rebuild");
                }
            }
            return "导入数据成功";
        } catch (Exception e) {
            e.printStackTrace();
            log.error("导入数据到hive失败的异常为:" + e);
        }
        return "导入数据失败";
    }
}
