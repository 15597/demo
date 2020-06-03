package com.example.demo.Controller;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class test {
    public static void main(String[] args) {

        File file = null;
        FileWriter fw = null;
        file = new File("D:\\test1.txt");
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            fw = new FileWriter(file);
            for(int i = 1;i <=30000000;i++){
                if(i<15000000){
                    fw.write(i +","+i +","+i +","+i +","+i +","+i +","+i +
                            ","+i +","+i +","+i +","+i +","+i +","+i +","+i +","+i +"," + i +","
                            +i +","+20 +","+i +","+i +","+i +","+i +","+i +","+i +","+i +","+i +","+
                            i +","+i +","+i +","+10);//向文件中写内容
                }else {
                    fw.write(i +","+i +","+i +","+i +","+i +","+i +","+i +
                            ","+i +","+i +","+i +","+i +","+i +","+i +","+i +","+i +"," + i +","
                            +i +","+40 +","+i +","+i +","+i +","+i +","+i +","+i +","+i +","+i +","+
                            i +","+i +","+i +","+30);//向文件中写内容
                }
                fw.write("\n");
                fw.flush();
            }
            System.out.println("写数据成功！");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            if(fw != null){
                try {
                    fw.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
}
