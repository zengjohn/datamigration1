package com.example.moveprog.exception;

/**
 * 专用于“作业被停止”的控制流异常
 */
public class JobStoppedException extends RuntimeException {
    public JobStoppedException(String message) {
        super(message);
    }
}