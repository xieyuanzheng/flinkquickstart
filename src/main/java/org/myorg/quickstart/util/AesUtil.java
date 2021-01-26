package org.myorg.quickstart.util;

import java.security.SecureRandom;
import java.util.Calendar;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

/**
 * AES加解密
 *
 * @author 朽木
 * @since 2018/12/28
 */
public class AesUtil {

    /**
     * AES 加密操作
     *
     * @param content  待加密内容
     * @param password 加密密码
     * @return 返回Base64转码后的加密数据
     */
    public static String encrypt(String content, String password) {
        try {

            // 创建密码器
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            byte[] byteContent = content.getBytes("utf-8");
            // 初始化为加密模式的密码器
            cipher.init(Cipher.ENCRYPT_MODE, getSecretKey(password));
            // 加密
            byte[] result = cipher.doFinal(byteContent);
            //通过Base64转码返回
            return Base64.encodeBase64String(result);

        } catch (Exception e) {
            System.out.println("AES加密异常:" + e);
        }
        return null;
    }

    /**
     * AES 解密操作
     *
     * @param content  待解密内容
     * @param password 解密密码
     * @return 返回Base64转码后的解密数据
     */
    public static String decrypt(String content, String password) {

        try {

            //实例化
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            //使用密钥初始化，设置为解密模式
            cipher.init(Cipher.DECRYPT_MODE, getSecretKey(password));
            //执行操作
            byte[] result = cipher.doFinal(Base64.decodeBase64(content));
            return new String(result, "utf-8");

        } catch (Exception e) {
            System.out.println("AES解密异常:" + e);
        }

        return null;
    }

    /**
     * 生成加密秘钥
     *
     * @return SecretKeySpec
     */
    private static SecretKeySpec getSecretKey(final String password) {
        //返回生成指定算法密钥生成器的 KeyGenerator 对象
        KeyGenerator kg = null;

        try {
            kg = KeyGenerator.getInstance("AES");
            SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
            secureRandom.setSeed(password.getBytes());
            //AES 要求密钥长度为 128
            kg.init(128, secureRandom);
            //生成一个密钥
            SecretKey secretKey = kg.generateKey();
            // 转换为AES专用密钥
            return new SecretKeySpec(secretKey.getEncoded(), "AES");
        } catch (Exception e) {
            System.out.println("生成加密秘钥异常:" + e);
        }

        return null;
    }

    public static void main(String[] args) {
        String userId = "666";
        String userName = "朽木";
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, 30);
        String expire = calendar.getTimeInMillis() + "";

        String data = userId + "_" + userName + "_" + expire;

        String token = encrypt(data, "www.yocoder.cn");

        System.out.println(token);
    }
}