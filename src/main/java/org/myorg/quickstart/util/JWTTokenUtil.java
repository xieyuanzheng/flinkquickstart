package org.myorg.quickstart.util;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;

/**
 * jwt token工具类
 *
 * @author 朽木
 * @since 2018/10/18
 */
public class JWTTokenUtil {

    /**
     * token秘钥，请勿泄露
     */
    private static final String SECRET = "www.yocoder.cn";
    /**
     * token 过期时间单位：天
     */
    private static final int expireUnit = Calendar.DATE;
    /**
     * token 过期时间: 30
     */
    private static final int expireDate = 30;

    /**
     * 获取token
     *
     * @param userId   登录用户id
     * @param userName 登录用户名
     * @return String
     */
    public static String getToken(String userId, String userName) {
        Calendar now = Calendar.getInstance();
        now.add(expireUnit, expireDate);
        Date expireTime = now.getTime();

        Map<String, Object> map = new HashMap<>();
        map.put("alg", "HS256");
        map.put("typ", "JWT");

        return JWT.create().withHeader(map)                            // header
                .withClaim("userId", userId)
                .withClaim("userName", userName)
                .withClaim("expireTime", expireTime)
                .sign(Algorithm.HMAC256(SECRET));                      // signature
    }

    /**
     * 解密Token,返回null则token校验失败
     *
     * @param token 要解密的token
     * @return Map
     */
    public static Map<String, Claim> verifyToken(String token) {
        try {
            JWTVerifier verifier = JWT.require(Algorithm.HMAC256(SECRET)).build();
            DecodedJWT jwt = verifier.verify(token);
            return jwt == null ? null : jwt.getClaims();
        } catch (Exception e) {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        String token = getToken("666", "朽木");
        System.out.println(token);
    }
}