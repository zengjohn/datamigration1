package com.example.moveprog;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class TestDataGenerator {
    // 基础配置
    private static final String OUT_DIR = "testdata";
    // 您的项目使用的是 ICU4J 的 x-IBM1388
    private static final String CHARSET_IBM = "x-IBM1388";
    private static final String CHARSET_UTF8 = "UTF-8";

    public static void main(String[] args) throws Exception {
        try {
            Files.createDirectories(Paths.get(OUT_DIR));
            System.out.println(">>> 开始生成测试数据，输出目录: " + OUT_DIR);

            generateBasic();
            generateCategoryA_Basic();
            generateCategoryB_CsvFormat();
            generateCategoryC_BusinessRules();
            generateCategoryD_Performance(100_000); // 生成10万行做演示，可改为 10_000_000
            generateCategoryE_Exceptions();
            generateCategoryF_Tunneling(); // 【新增】针对 FastEscapeHandler

            System.out.println(">>> 所有测试文件生成完毕！");

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("生成失败，请检查是否引入了 icu4j 依赖");
        }
    }

    private static void generateBasic() throws IOException {
        String filename = OUT_DIR + "/test_ibm1388.csv";
        // 1. 模拟数据：包含普通中文、英文、以及"生僻字转义符"(测试 Tunneling 功能)
        // 对应 FastEscapeHandler 的逻辑: \2CC56\ 会被还原
        String content = "user_id,user_name,balance\n" +
                "1001,张三,100.00\n" +
                "1002,李四_普通,200.50\n" +
                "1003,王\\2CC56\\五,888.88"; // 测试转义: \2CC56\

        writeWithEncoding(filename, content, CHARSET_IBM);
        System.out.println("[A] 基础编码文件生成: " + filename);
    }

    // ==========================================
    // A. 基础与编码验证
    // ==========================================
    private static void generateCategoryA_Basic() throws IOException {
        String filename = OUT_DIR + "/A_Basic_Encoding.csv";
        // 包含：英文、普通中文、数字
        StringBuilder sb = new StringBuilder();
        sb.append("user_id,user_name,balance\n");
        sb.append("A001,John Doe,100.00\n");
        sb.append("A002,张三_测试,200.50\n");
        sb.append("A003,李四_End,999.99");

        writeWithEncoding(filename, sb.toString(), CHARSET_IBM);
        System.out.println("[A] 基础编码文件生成: " + filename);
    }

    // ==========================================
    // B. 数据格式边界验证 (CSV 解析健壮性)
    // ==========================================
    private static void generateCategoryB_CsvFormat() throws IOException {
        String filename = OUT_DIR + "/B_Csv_Format_Edge.csv";
        StringBuilder sb = new StringBuilder();
        sb.append("user_id,user_name,balance\n");

        // 1. 字段含分隔符 (需引号包裹)
        sb.append("B001,\"Doe, John\",100.00\n");

        // 2. 字段含换行符 (Univocity 需支持多行模式)
        sb.append("B002,\"张三\n换行测试\",200.00\n");

        // 3. 字段含引号 (需双重转义) -> 实际显示为: 王"五"
        sb.append("B003,\"王\"\"五\"\"\",300.00\n");

        // 4. 空字段与空行
        sb.append("B004,,0.00\n"); // user_name 为空
        sb.append(",,\n");         // 全空行

        // 5. 超长字段 (15KB 字符串)
        String longText = generateRandomString(15000);
        sb.append("B005,\"Long_Start_").append(longText).append("_End\",500.00\n");

        writeWithEncoding(filename, sb.toString(), CHARSET_IBM);
        System.out.println("[B] CSV格式边界文件生成: " + filename);
    }

    // ==========================================
    // C. 业务规则边界验证
    // ==========================================
    private static void generateCategoryC_BusinessRules() throws IOException {
        String filename = OUT_DIR + "/C_Business_Rules.csv";
        StringBuilder sb = new StringBuilder();
        sb.append("user_id,user_name,balance\n");

        // 1. 金额极值
        sb.append("C001,Max_Value,9999999999.99\n");
        sb.append("C002,Min_Value,-9999999999.99\n");
        sb.append("C003,High_Precision,0.123456789\n"); // 看看数据库 Decimal(10,2) 是否报错或截断

        // 2. 空格敏感 (前导/后导空格)
        sb.append("C004,  Space_Test  ,100.00\n");

        // 3. 特殊ID
        sb.append("0000,Zero_ID,0\n");

        writeWithEncoding(filename, sb.toString(), CHARSET_IBM);
        System.out.println("[C] 业务规则文件生成: " + filename);
    }

    // ==========================================
    // D. 批量与性能验证
    // ==========================================
    private static void generateCategoryD_Performance(int rows) throws IOException {
        String filename = OUT_DIR + "/D_Performance_" + rows + ".csv";
        // 使用 BufferedWriter 避免内存溢出
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(filename), CHARSET_IBM))) {

            writer.write("user_id,user_name,balance\n");
            Random rand = new Random();

            for (int i = 0; i < rows; i++) {
                // 模拟真实分布：大部分是短数据，偶尔有长数据
                String name = (i % 100 == 0) ? "Name_" + generateRandomString(500) : "User_" + i;
                String line = String.format("D%07d,%s,%.2f\n", i, name, rand.nextDouble() * 10000);
                writer.write(line);
            }
        }
        System.out.println("[D] 性能测试文件生成 (" + rows + "行): " + filename);
    }

    // ==========================================
    // E. 异常与容错验证 (二进制破坏)
    // ==========================================
    private static void generateCategoryE_Exceptions() throws IOException {
        // E1. 错误编码混杂 (IBM1388 中混入 UTF-8)
        File f1 = new File(OUT_DIR + "/E1_Mixed_Encoding.csv");
        try (FileOutputStream fos = new FileOutputStream(f1)) {
            fos.write("user_id,user_name,balance\n".getBytes(CHARSET_IBM));
            fos.write("E001,正常IBM1388,100\n".getBytes(CHARSET_IBM));
            fos.write("E002,我是UTF8乱入,200\n".getBytes(CHARSET_UTF8)); // 这里会产生乱码
            fos.write("E003,正常IBM1388,300\n".getBytes(CHARSET_IBM));
        }

        // E2. 错误引号 (Unclosed Quote) - 导致解析器吞掉后续所有行
        String f2 = OUT_DIR + "/E2_Unclosed_Quote.csv";
        StringBuilder sb = new StringBuilder();
        sb.append("user_id,user_name,balance\n");
        sb.append("E004,\"Unclosed Quote Start,100.00\n"); // 缺右引号
        sb.append("E005,Should_Be_Swallowed,200.00\n");
        writeWithEncoding(f2, sb.toString(), CHARSET_IBM);

        // E3. 文件截断 (写入一半突然结束)
        File f3 = new File(OUT_DIR + "/E3_Truncated.csv");
        try (FileOutputStream fos = new FileOutputStream(f3)) {
            fos.write("user_id,user_name,bal".getBytes(CHARSET_IBM)); // Header都没写完
        }

        System.out.println("[E] 异常容错文件生成: E1, E2, E3");
    }

    // ==========================================
    // F. [新增] 转义穿透 (Tunneling) 验证
    // ==========================================
    private static void generateCategoryF_Tunneling() throws IOException {
        String filename = OUT_DIR + "/F_Tunneling_Handler.csv";
        StringBuilder sb = new StringBuilder();
        sb.append("user_id,user_name,balance\n");

        // 1. 标准生僻字转义 (假设 \2CC56\ 代表一个生僻字)
        // 期望：入库后不再包含 \2CC56\，而是对应的 Unicode 字符(如果转换支持) 或 保持原样(取决于配置)
        sb.append("F001,张\\2CC56\\三,100.00\n");

        // 2. 连续转义
        sb.append("F002,A\\2CC56\\B\\2CC57\\C,200.00\n");

        // 3. 假转义 (格式不对，FastEscapeHandler 应该忽略)
        sb.append("F003,普通斜杠\\ABC,300.00\n");

        // 4. 边界情况：行尾转义不闭合
        sb.append("F004,未闭合\\2CC56,400.00\n");

        writeWithEncoding(filename, sb.toString(), CHARSET_IBM);
        System.out.println("[F] 转义穿透测试文件生成: " + filename);
    }

    // --- 工具方法 ---
    private static void writeWithEncoding(String path, String content, String encoding) throws IOException {
        File file = new File(path);
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), encoding))) {
            writer.write(content);
            System.out.println("生成成功: " + path);
            System.out.println("文件大小: " + file.length() + " 字节");
        }
    }

    private static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

}