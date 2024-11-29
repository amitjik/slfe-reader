package in.indigo.processor;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.ObjectMapper;

import in.indigo.entity.InvSkyExtract;
import in.indigo.util.CsvMappingConfig;
import in.indigo.util.CsvUtil;
import in.indigo.util.CustomObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class CSVValidator implements Processor {

    @Inject
    CsvMappingConfig csvMappingConfig;

    @Inject
    CsvUtil csvUtil;

    @ConfigProperty(name = "pnr-dump-file-path")
    String destinationPath;

    ObjectMapper customMapper = CustomObjectMapper.createCustomObjectMapper();
    static Map<String, String> csvColumnSetKeyAsNormalised = new HashMap<>();
    static Map<String, String> columnToFieldMap = new HashMap<>();
    final List<String> commaCheckFields = Arrays.asList("customername", "6eregisteredaddress");
    final List<String> dateCheckFields = Arrays.asList("originalbookingdate", "transactiondate", "flightdate");

    @Override
    public void process(Exchange exchange) throws Exception {

        Map<String, String> csvRow = exchange.getIn().getBody(Map.class);
        Integer rowNumber = (Integer) exchange.getProperty("CamelSplitIndex") + 1;

        if (rowNumber == 1) {
            System.out.println("3333333333333333333333333333333333333333333333333333333");
            columnToFieldMap = csvMappingConfig.mapping();

            for (String key : csvRow.keySet()) {
                String normalizedKey = key.trim().toLowerCase().replace(" ", "");
                csvColumnSetKeyAsNormalised.put(normalizedKey, key);
            }
            for (Map.Entry<String, String> entry : columnToFieldMap.entrySet()) {
                // String fieldName = entry.getKey(); // InvSkyExtract field
                String headerValue = entry.getValue(); // The CSV header
                // Normalize the key by removing spaces and converting to lower case
                String normalizedKeyToCheck = headerValue.trim().toLowerCase().replace(" ", "");
                if (!csvColumnSetKeyAsNormalised.containsKey(normalizedKeyToCheck) && rowNumber == 1) {
                    log.info("expected header missing: " + headerValue);
                    throw new IllegalArgumentException("Missing column header name is: " + headerValue);
                }

            }

        }

        // comma check
        for (String str : commaCheckFields) {
            String value = csvRow.get(csvColumnSetKeyAsNormalised.get(str));
            csvUtil.isCommaPresent(value, rowNumber, csvColumnSetKeyAsNormalised.get(str));
        }
        // date check
        for (String str : dateCheckFields) {
            String value = csvRow.get(csvColumnSetKeyAsNormalised.get(str));
            csvUtil.isValidDateFormat(value, rowNumber, csvColumnSetKeyAsNormalised.get(str));
        }
        long currentTimeMillis = System.currentTimeMillis();
        String pnr = csvRow.get(csvColumnSetKeyAsNormalised.get("pnr"));
        if (pnr == null || pnr.isEmpty()) {
            log.info("Given pnr is null or empty string at record number: " + rowNumber);
            throw new RuntimeException("Given pnr is null or empty string at record number: " + rowNumber);

        }

        csvRow.put("Id",
                Long.toString(currentTimeMillis) + pnr
                        + exchange.getProperty("CamelSplitIndex"));
        exchange.getIn().setBody(csvRow);
    }

}
