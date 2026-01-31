package com.example.moveprog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class BigTestDataGenerator {
    private static final String OUT_DIR = "/data/testdata";
    private static final String CHARSET_UTF8 = "UTF-8";

    private static final Random random = new Random();
    private static final String[] FIRST_NAMES = {"张", "李", "王", "刘", "陈", "杨", "赵", "黄", "周", "吴", "孙", "朱", "马", "胡", "郭", "林", "何", "高", "梁", "郑"};
    private static final String[] LAST_NAMES = {"伟", "芳", "娜", "秀英", "敏", "静", "磊", "洋", "勇", "艳", "杰", "军", "强", "磊", "超", "鹏", "华", "平", "建", "明"};
    private static final String[] REMARKS = {"员工", "经理", "主管", "工程师", "技术员", "销售", "客服", "财务", "行政", "人事"};

    @BeforeAll
    public static void setUp() throws IOException {
        Files.createDirectories(Paths.get(OUT_DIR));
        System.out.println(">>> 开始生成测试数据，输出目录: " + OUT_DIR);
    }

    /**
     * 字段说明：
     *
     * i - 序号，从1到10000000
     * name - 姓名，随机生成的中文姓名
     * salary - 工资，3000.00到50000.00之间的随机值
     * high - 身高，1.50到2.00米之间的随机值
     * weight - 体重，40.0到100.0公斤之间的随机值
     * birth1 - 出生日期，1960-01-01到2005-12-31之间的随机日期
     * birth2 - 出生详细时间，包含日期和微秒级的精确时间
     * luchAt - 午饭时间，11:00:00到14:00:00之间的随机时间
     * remark - 备注，随机生成的工作职位和级别
     *
     * @throws IOException
     */
    @Test
    public void test_1() throws IOException {
        System.out.println("开始生成测试数据...");
        long startTime = System.currentTimeMillis();

        String filename = OUT_DIR + "/test_bigtable.utf8.csv";
        File file = new File(filename);

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), CHARSET_UTF8))) {
            int i = 0;
            for (i = 0; i < 10_000_000; i++) {
                int index = i+1;
                String rowData = generateRowData(index);
                writer.write(rowData);
                writer.newLine();
                if (i % 100_000 == 0) {
                    writer.flush();
                    System.out.println("i= " + i);
                }
            }
            System.out.println("i= " + i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("数据生成完成！耗时: " + (endTime - startTime) / 1000 + " 秒");

        System.out.println("请用linux命令: iconv -f UTF-8 -t IBM1388 test_bigtable.utf8.csv -o test_bigtable.csv"+" 去转换成ibm1388编码");

    }

    private static String generateRowData(int index) {
        // 1. i (int) - index
        int i = index;

        // 2. name (varchar(100)) - 姓名
        String name = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)] +
                LAST_NAMES[random.nextInt(LAST_NAMES.length)];

        // 3. salary (decimal(19,2)) - 工资
        // 范围: 3000.00 - 50000.00
        double salary = 3000 + random.nextDouble() * 47000;
        String salaryStr = String.format("%.2f", salary);

        // 4. high (float) - 身高
        // 范围: 1.50 - 2.00 米
        float high = 1.5f + random.nextFloat() * 0.5f;

        // 5. weight (double) - 体重
        // 范围: 40.0 - 100.0 公斤
        double weight = 40.0 + random.nextDouble() * 60.0;

        // 6. birth1 (Date) - 出生时间
        // 生成1960-01-01到2005-12-31之间的随机日期
        LocalDate startDate = LocalDate.of(1960, 1, 1);
        LocalDate endDate = LocalDate.of(2005, 12, 31);
        long startEpochDay = startDate.toEpochDay();
        long endEpochDay = endDate.toEpochDay();
        long randomDay = startEpochDay + random.nextInt((int)(endEpochDay - startEpochDay));
        LocalDate birth1 = LocalDate.ofEpochDay(randomDay);

        // 7. birth2 (Datetime(6)) - 出生详细时间
        // 同一天，加上随机时间（包括微秒）
        int hour = random.nextInt(24);
        int minute = random.nextInt(60);
        int second = random.nextInt(60);
        int nano = random.nextInt(1000000); // 微秒（纳秒/1000）
        LocalDateTime birth2 = LocalDateTime.of(birth1.getYear(), birth1.getMonthValue(),
                birth1.getDayOfMonth(), hour, minute, second, nano * 1000);

        // 格式化datetime(6): yyyy-MM-dd HH:mm:ss.SSSSSS
        String birth2Str = String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",
                birth2.getYear(), birth2.getMonthValue(), birth2.getDayOfMonth(),
                birth2.getHour(), birth2.getMinute(), birth2.getSecond(),
                birth2.getNano() / 1000);

        // 8. luchAt (Time) - 午饭时间
        // 11:00:00 - 14:00:00 之间的随机时间
        int lunchHour = 11 + random.nextInt(3); // 11-13
        int lunchMinute = random.nextInt(60);
        int lunchSecond = random.nextInt(60);
        LocalTime luchAt = LocalTime.of(lunchHour, lunchMinute, lunchSecond);

        // 9. remark (text) - 注释
        String remark = REMARKS[random.nextInt(REMARKS.length)] +
                (random.nextInt(10) + 1) + "级";

        // 格式化为CSV格式，用逗号分隔
        return String.format("%d,%s,%s,%.2f,%.2f,%s,%s,%s,%s",
                i,
                name,
                salaryStr,
                high,
                weight,
                birth1.toString(),  // yyyy-MM-dd
                birth2Str,
                luchAt.toString(),
                remark
        );
    }
}