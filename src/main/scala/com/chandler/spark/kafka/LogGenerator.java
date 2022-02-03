package com.chandler.spark.kafka;

import java.util.Date;
import java.util.Random;

public class LogGenerator {
    public LogGenerator() {
    }

    public static String generate() {
        String[] os = new String[]{"Android", "iOS", "WP"};
        String[] version = new String[]{"1.1.0", "1.1.1", "1.1.2", "1.1.3"};
        long[] catagory = new long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L};
        String[] user = new String[]{"user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"};
        Random random = new Random();

        Log log = new Log();
        log.setUser(user[random.nextInt(user.length)]);
        log.setOs(os[random.nextInt(os.length)]);
        log.setVersion(version[random.nextInt(version.length)]);
        log.setIp(getRandomIp());
        long cata = catagory[random.nextInt(catagory.length)];
        log.setCatagory(cata);
        log.setNewId(cata + "" + random.nextInt(10));
        long start = (new Date()).getTime();
        log.setStart(start);
        log.setEnd((long) (random.nextInt(10) * 1000));
        log.setTraffic((long) (5000 + random.nextInt(1000)));
        return log.toString();
    }

    private static String getRandomIp() {
        int[][] range = new int[][]{{607649792, 608174079}, {1038614528, 1039007743}, {1783627776, 1784676351}, {2035023872, 2035154943}, {2078801920, 2079064063}, {-1950089216, -1948778497}, {-1425539072, -1425014785}, {-1236271104, -1235419137}, {-770113536, -768606209}, {-569376768, -564133889}};
        Random rdint = new Random();
        int index = rdint.nextInt(10);
        String ip = num2ip(range[index][0] + (new Random()).nextInt(range[index][1] - range[index][0]));
        return ip;
    }

    private static String num2ip(int ip) {
        int[] b = new int[4];
        String x = "";
        b[0] = ip >> 24 & 255;
        b[1] = ip >> 16 & 255;
        b[2] = ip >> 8 & 255;
        b[3] = ip & 255;
        x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "." + Integer.toString(b[3]);
        return x;
    }
}
