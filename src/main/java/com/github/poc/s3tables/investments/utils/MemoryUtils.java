package com.github.poc.s3tables.investments.utils;

import lombok.extern.slf4j.Slf4j;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

@Slf4j
public class MemoryUtils {
    public static void checkHeapMemory() {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        long usableMemoryInBytes = memoryBean.getHeapMemoryUsage().getCommitted();
        log.info("S3 Table POC - Maximum Heap Memory Allocated: {}", usableMemoryInBytes);
    }
}
