-- 1. 主任务表 (配置监控目录和目标库)
CREATE TABLE qianyi_main (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL COMMENT '作业唯一标识',
    source_dir VARCHAR(255) NOT NULL COMMENT '源目录(监控目录)',

    -- TDSQL 连接信息
    target_jdbc_url VARCHAR(255) NOT NULL,
    target_user VARCHAR(100) NOT NULL,
    target_password VARCHAR(100) NOT NULL,
    target_table_name VARCHAR(100) NOT NULL,

    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否开启监控'
);

-- 2. 明细任务表 (每一次文件传输就是一个明细)
CREATE TABLE qianyi_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    main_id BIGINT NOT NULL COMMENT '关联主表ID',

    -- 核心文件信息
    ok_file_path VARCHAR(255) NOT NULL COMMENT 'OK文件全路径',
    csv_file_path VARCHAR(255) COMMENT '解析OK文件后得到的大机CSV路径',

    -- 状态: N(待转码), WL(待装入), WV(待验证), PASS(成功), FAIL(失败)
    status VARCHAR(20) DEFAULT 'N',

    -- 过程数据
    split_files_json TEXT COMMENT '转码后生成的拆分文件列表(JSON格式)',

    -- 审计与日志
    error_msg TEXT COMMENT '失败时的错误日志',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_status (status)
);