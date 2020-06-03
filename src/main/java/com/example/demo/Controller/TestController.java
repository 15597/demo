package com.example.demo.Controller;

import com.example.demo.service.testService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private com.example.demo.service.testService testService;


    @GetMapping("/{sqlStr}")
    public String MoveDateToLocal(@PathVariable String sqlStr) {
        if (!sqlStr.isEmpty()) {
            boolean flag = testService.moveDateToLocal(sqlStr);
            if (flag) {
                return "执行成功";
            } else {
                return "执行失败";
            }
        }
        return "执行失败,参数不能为空";
    }

    @GetMapping("/{fileName}/{loadType}")
    public String moveToHive(@PathVariable String fileName, @PathVariable Integer loadType) {
        if (fileName != null && loadType != null) {
            return testService.moveToHive(fileName, loadType);
        } else {
            return "请输入正确的参数";
        }
    }

}
