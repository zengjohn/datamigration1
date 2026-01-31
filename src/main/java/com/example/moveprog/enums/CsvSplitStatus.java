package com.example.moveprog.enums;

public enum CsvSplitStatus {
    WAIT_LOAD("待装载"),
    LOADING("装载中"),
    FAIL_LOAD("装载失败"),
    WAIT_VERIFY("待验证"),
    VERIFYING("验证中"),
    FAIL_VERIFY("验证失败"),
    PASS("通过");

    private String desc;
    CsvSplitStatus(String desc) { this.desc = desc; }

    /**
     * 核心逻辑：判断能否从当前状态(this) 流转到 目标状态(target)
     */
    public boolean canTransitionTo(CsvSplitStatus target) {
        // 如果是自身转自身，通常允许（比如重试时更新一下时间）
        if (this == target) return true;

        // 允许重试逻辑: FAIL -> WAIT
        if ((this == FAIL_LOAD && target == WAIT_LOAD) ||
                (this == FAIL_VERIFY && target == WAIT_VERIFY)) {
            return true;
        }

        switch (this) {
            // 1. [待装载] -> 只能变 [装载中]
            case WAIT_LOAD: {
                return target == LOADING;
            }

            // 2. [装载中] -> 成功变 [待校验]，失败变 [装载失败]
            case LOADING: {
                // 允许流转到：
                // 1. WAIT_VERIFY (成功)
                // 2. FAIL_LOAD (失败)
                // 3. WAIT_LOAD (关键修改：允许被僵尸检测机制重置回待装载，以便重新调度)
                return target == WAIT_VERIFY || target == FAIL_LOAD || target == WAIT_LOAD;
            }

            // 3. [待校验] 或 [校验失败] -> 只能变 [校验中]
            // 注意：如果允许[校验失败]后重新[装载]，这里要加上 target == LOADING
            case WAIT_VERIFY: {
                return target == VERIFYING;
            }

            // 4. [校验中] -> 成功变 [PASS]，失败变 [校验失败]
            case VERIFYING: {
                return target == PASS || target == FAIL_VERIFY;
            }
            default: return false;
        }
    }

}