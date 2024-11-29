package in.indigo.util;

import java.util.Map;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "csv")

// @ConfigProperties(prefix = "csv.mapping")
public interface CsvMappingConfig {
    Map<String, String> mapping();
}
