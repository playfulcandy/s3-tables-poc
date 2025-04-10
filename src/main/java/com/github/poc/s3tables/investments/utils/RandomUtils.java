package com.github.poc.s3tables.investments.utils;

import java.util.*;

public class RandomUtils {

    public static String createRandomAlphaNumeric(int length) {
        String alphaNumerics = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        String uppercases = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder result = new StringBuilder("" + uppercases.charAt((int) (Math.random() * uppercases.length())));
        for (int i = 1; i < length; i++) {
            result.append(alphaNumerics.charAt((int) (Math.random() * alphaNumerics.length())));
        }
        return result.toString();
    }

    public static double createRandomPrice(double lowerbound, double upperbound) {
        double difference = upperbound - lowerbound;
        double random = lowerbound + Math.random() * difference;
        return ((int) (random * 100)) / 100.0;
    }

    public static Set<String> getRandomSubset(int max, Collection<String> source) {
        List<String> listedSource = new ArrayList<>(source);
        Set<String> selection = new HashSet<>();

        for (int i = 0; i < max; i++) {
            selection.add(listedSource.get((int) (Math.random() * source.size())));
        }
        return selection;
    }
}
