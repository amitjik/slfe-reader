package in.indigo.processor;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.ObjectMapper;

import in.indigo.entity.InvSkyExtract;
import in.indigo.util.CsvMappingConfig;
import in.indigo.util.CustomObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ObjectToMapProcessor implements Processor {

    @ConfigProperty(name = "slfe.column.order")
    String columnOrder;

    @Inject
    CsvMappingConfig csvMappingConfig;

    @ConfigProperty(name = "pnr-dump-file-path")
    String destinationPath;

    ObjectMapper customMapper = CustomObjectMapper.createCustomObjectMapper();

    @Override
    public void process(Exchange exchange) throws Exception {
        Map<String, String> csvRow = exchange.getIn().getBody(Map.class);

        Map<String, String> csvColumnSetKeyAsNormalised = new HashMap<>();
        LinkedHashMap<String, String> csvValue = new LinkedHashMap<>();

        for (String key : csvRow.keySet()) {
            String normalizedKey = key.trim().toLowerCase().replace(" ", "");
            csvColumnSetKeyAsNormalised.put(normalizedKey, key);
            csvValue.put(key, csvRow.get(key));
        }

        exchange.getIn().setHeader("CamelFileName", csvRow.get(csvColumnSetKeyAsNormalised.get("pnr")) + ".csv");

        String transactionDate = csvRow.get(csvColumnSetKeyAsNormalised.get("transactiondate")).toString().replace("/",
                "_");
        String directoryPath = destinationPath + "/" + transactionDate;
        exchange.getIn().setHeader("directoryPath", directoryPath);
        String str1 = directoryPath + "/" + csvRow.get(csvColumnSetKeyAsNormalised.get("pnr")) + ".csv";
        // String str = customMapper.writeValueAsString(entry.getValue());
        // List<InvSkyExtract> slfeData = Arrays
        // .asList(customMapper.readValue(str, InvSkyExtract[].class));

        List<LinkedHashMap<String, String>> result = new ArrayList<>();

        LinkedHashMap<String, String> csvHeader = new LinkedHashMap<>();
        File file = new File(str1);
        if (!file.exists()) {
            // File doesn't exist, create it
            System.out.println("File does not exist, creating file...");
            for (String key : csvRow.keySet()) {
                csvHeader.put(key, key);
            }

            result.add(csvHeader);
        }

        // for (InvSkyExtract invSkyExtract : slfeData) {

        result.add(csvValue);
        // }

        exchange.getIn().setBody(result); // Set list of records as body
    }

}
