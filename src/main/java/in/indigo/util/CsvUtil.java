package in.indigo.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class CsvUtil {

    public String isValidDateFormat(String date, Integer rowNumber, String attributeName) {

        if (date == null || date.isEmpty()) {
            log.info("Given Date is null or empty string at record number: " + rowNumber + " for header: "
                    + attributeName);
            throw new RuntimeException("Given Date is null or empty string at record number: " + rowNumber
                    + " for header: " + attributeName);

        }
        // List<String> validFormat = Arrays.asList("M/dd/yyyy", "M/d/yyyy",
        // "MM/d/yyyy", "MM/dd/yyyy");

        List<String> validFormat = Arrays.asList("M/d/yyyy", "d/M/yyyy");
        // List<String> validFormat = Arrays.asList("yyyy/M/d", "yyyy/d/M");

        Boolean isValid = false;
        for (String string : validFormat) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(string);
            try {
                LocalDate parsedDate = LocalDate.parse(date, formatter);
                parsedDate.format(formatter);

                isValid = true;
            } catch (DateTimeParseException e) {

                isValid = false;
            }
            if (isValid)
                break;

        }
        if (!isValid) {
            log.info("Given Date where exception occure: " + date);
            throw new RuntimeException(
                    "Given date: " + date + ", Not a valid date formate at record number: " + rowNumber);
        } else
            return date;
    }

    public String isCommaPresent(String input, Integer rowNumber, String attributeName) {
        if (input == null || input.isEmpty()) {
            // log.info("Given string is null or empty string where exception occure");
            // throw new RuntimeException("Given string is null or empty string at record
            // number: "+ rowNumber + " for hader: "+attributeName);
            return input;
        }
        if (!input.contains(",")) {
            log.info("Given string where exception occure: " + input);
            throw new RuntimeException("Value: " + input + ", must have a comma at record number: " + rowNumber);
        }
        return input;
    }

}
