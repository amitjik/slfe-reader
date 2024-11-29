package in.indigo.processor;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

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
public class CsvProcessor implements Processor {

    @Inject
    CsvMappingConfig csvMappingConfig;

    @Inject
    CsvUtil csvUtil;

    ObjectMapper customMapper = CustomObjectMapper.createCustomObjectMapper();

    @Override
    public void process(Exchange exchange) throws Exception {

        Map<String, String> csvRow = exchange.getIn().getBody(Map.class);

        Map<String, String> csvColumnSetKeyAsNormalised = new HashMap<>();

        for (String key : csvRow.keySet()) {
            String normalizedKey = key.trim().toLowerCase().replace(" ", "");
            csvColumnSetKeyAsNormalised.put(normalizedKey, key);
        }


        Integer rowNumber = (Integer) exchange.getProperty("CamelSplitIndex");

        InvSkyExtract invSkyExtract = new InvSkyExtract();

        Map<String, String> columnToFieldMap = csvMappingConfig.mapping();

        List<String> commaCheckField = Arrays.asList("Customer Name", "6E RegisteredAddress");
        List<String> dateCheckField = Arrays.asList("Original Booking Date", "Transaction Date", "Flight Date");

        for (Map.Entry<String, String> entry : columnToFieldMap.entrySet()) {
            String fieldName = entry.getKey(); // InvSkyExtract field
            String headerValue = entry.getValue(); // The CSV header

            // Normalize the key by removing spaces and converting to lower case
            String normalizedKeyToCheck = headerValue.trim().toLowerCase().replace(" ", "");
            String csvHeader = null;
            if (csvColumnSetKeyAsNormalised.containsKey(normalizedKeyToCheck)) {
                csvHeader = csvColumnSetKeyAsNormalised.get(normalizedKeyToCheck);
            } else {
                log.info("expected header missing: " + headerValue);
                throw new IllegalArgumentException("column missing: " + headerValue);
            }

            if (csvHeader != null) {
                String value = csvRow.get(csvHeader);
                // if (dateCheckField.contains(csvHeader)) {
                //     csvUtil.isValidDateFormat(value, rowNumber);
                // }
                // if (commaCheckField.contains(csvHeader)) {
                //     csvUtil.isCommaPresent(value, rowNumber);
                // }
                // Get the corresponding field in the invSkyExtract class by the mapped field
                // name
                Field field = InvSkyExtract.class.getDeclaredField(fieldName);
                field.setAccessible(true); // Allow access to private fields

                // Handle different field types
                if (field.getType() == int.class) {
                    value = (value == null || value.isEmpty()) ? "0" : value;
                    field.setInt(invSkyExtract, Integer.parseInt(value));
                } else if (field.getType() == double.class) {
                    value = (value == null || value.isEmpty()) ? "0.0" : value;
                    field.setDouble(invSkyExtract, Double.parseDouble(value));
                } else if (field.getType() == boolean.class) {

                    value = (value == null || value.isEmpty() || value.equals("0")) ? "false" : "true";
                    field.setBoolean(invSkyExtract, Boolean.parseBoolean(value));
                } else if (field.getType() == long.class) {
                    value = (value == null || value.isEmpty()) ? "0" : value;
                    field.setLong(invSkyExtract, Long.parseLong(value));
                } else if (field.getType() == float.class) {
                    value = (value == null || value.isEmpty()) ? "0.0f" : value;
                    field.setFloat(invSkyExtract, Float.parseFloat(value));
                } else if (field.getType() == String.class) {
                    value = (value == null || value.isEmpty()) ? null : value;
                    field.set(invSkyExtract, value);
                } else {
                    // Handle other data types or custom objects if necessary
                    throw new IllegalArgumentException("Unsupported field type: " + field.getType());
                }

            } else {
                throw new IllegalArgumentException("column missing: " + headerValue);
            }
        }
        // System.out.println("Mapped invSkyExtract: " +
        // customMapper.writeValueAsString(invSkyExtract));
        long currentTimeMillis = System.currentTimeMillis();
        invSkyExtract.setId(Long.toString(currentTimeMillis)+invSkyExtract.getPnr()+exchange.getProperty("CamelSplitIndex"));
        exchange.getIn().setBody(invSkyExtract);
    }

}
