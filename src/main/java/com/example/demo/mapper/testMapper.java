package com.example.demo.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.LinkedHashMap;
import java.util.List;


public interface testMapper{
    @Select("${sqlStr}")
    List<LinkedHashMap<String,Object>> getDates(@Param(value = "sqlStr") String sqlStr);

    //获取hive表字段的集合
    @Select("select filedName,filedType from hive_table_fields where table_id = (SELECT id FROM hive_table_config WHERE table_name = '${table_name}')")
    List<LinkedHashMap<String,Object>> getFiledList(@Param(value = "table_name") String table_name);
    //获取hive表索引的集合
    @Select("select index_Name,filed_Name from hive_table_indexes where table_id = (SELECT id FROM hive_table_config WHERE table_name = '${table_name}')")
    List<LinkedHashMap<String,Object>> getIndexList(@Param(value = "table_name") String table_name);
    //获取hive表分区的集合
    @Select("select partitionName,partitionType from hive_table_partitions where table_id = (SELECT id FROM hive_table_config WHERE table_name = '${table_name}')")
    List<LinkedHashMap<String,Object>> getPartitionsList(@Param(value = "table_name") String table_name);
    //获取路径名以及加载的形式
    @Select("select filePath,loadFormat from hive_table_config where table_name = '${table_name}'")
    List<LinkedHashMap<String,Object>> getConfigList(@Param(value = "table_name") String table_name);
    //获取所有的路径名
    @Select("select fileName from hive_table_config")
    List<String> getFileNameList();
    //通过路径名获取到表明
    @Select("select table_name from hive_table_config where fileName ='${fileName}'")
    String getTableName(@Param(value = "fileName") String fileName);


}

