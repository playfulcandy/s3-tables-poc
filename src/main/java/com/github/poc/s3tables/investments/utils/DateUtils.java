package com.github.poc.s3tables.investments.utils;

import org.apache.hadoop.shaded.org.apache.commons.lang3.math.NumberUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtils {

    public static long getTime() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        LocalDateTime time = LocalDateTime.now();

        return NumberUtils.toLong(time.format(formatter));
    }
}
