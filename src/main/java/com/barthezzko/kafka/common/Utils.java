package com.barthezzko.kafka.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {

    private static Map<String, String> config = new HashMap<>();
    private static Map<String, Double> refdata = new HashMap<>();
    private final static Logger logger = LoggerFactory.getLogger(Utils.class);

    static {
        loadAppConfig();
        loadRefdata();
    }

    private static void loadRefdata() {
        try {
            Files.readAllLines(Paths.get("src/main/resources/snp_500.txt")).forEach(line->{
                String[] tokens = line.split("\t");
                refdata.put(tokens[0], Double.valueOf(tokens[2]));
            });
        } catch (IOException e) {
            logger.error("Error loading reference data", e);
            e.printStackTrace();
        }
    }

    private static void loadAppConfig() {
        try (InputStream input = new FileInputStream("src/main/resources/config.properties")) {
            Properties properties = new Properties();
            properties.load(input);
            properties.forEach((k, v) -> {
                config.put(k.toString(), v.toString());
            });
        } catch (IOException e) {
            logger.error("Error loading config", e);
            e.printStackTrace();
        }
    }

    public static Properties getPropertiesByPrefixAndStripOffPrefix(String prefix) {
        Properties props = new Properties();
        getConfigMapByPrefix(prefix)
                .forEach((key, value)->{
                    String cleanKey = key.substring(prefix.length());
                    props.put(cleanKey, value);
                });
        return props;
    }

    public static Map<String, String> getConfigMapByPrefix(String prefix) {
        return Collections.unmodifiableMap(config.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public static String getPropertyByName(String key) {
        return config.get(key);
    }

    public static Map<String, String> getConfigMap() {
        LinkedHashMap<String, String> sortedMap = new LinkedHashMap<>();
        getConfigMapByPrefix("").entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
        return sortedMap;
    }

    public static Map<String, Double> getRefdata(){
        return Collections.unmodifiableMap(refdata);
    }
}
