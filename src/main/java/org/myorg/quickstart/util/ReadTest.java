package org.myorg.quickstart.util;

import java.io.*;

public class ReadTest {

    public  void txt2String(){

        try {
            //绝对路径
            String filePath = "/*/work/project/java/flinkquickstart/src/main/resources/users.2.txt";
            FileInputStream fin = new FileInputStream(filePath);
            InputStreamReader reader = new InputStreamReader(fin);
            BufferedReader br1 = new BufferedReader(reader);
            //读取resource
            InputStream path = this.getClass().getResourceAsStream("/users.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(path));
            String strCmp = "";
            while ((strCmp = br.readLine())!=null){
                System.out.println(strCmp);
            }
            br.close();
            br1.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        ReadTest readTest = new ReadTest();
        readTest.txt2String();
    }

}
