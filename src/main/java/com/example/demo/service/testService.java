package com.example.demo.service;

public interface testService {

    boolean moveDateToLocal(String sqlStr);

    String moveToHive(String fileName,Integer loadType);
}
