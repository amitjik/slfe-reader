package in.indigo.aggregationStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.ObjectMapper;

import in.indigo.entity.GstDistributionConfiguration;
import in.indigo.entity.InvSkyExtract;
import in.indigo.resource.Data;
import in.indigo.resource.Report;
import in.indigo.util.RepositoryUtil;
import in.indigo.util.ResourceUtil;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class MyOrderStrategy implements AggregationStrategy {

        @Inject
        ResourceUtil util;

        @Inject
        RepositoryUtil repositoryUtil;

        @Inject
        ObjectMapper mapper;

        static Map<String, String> csvColumnSetKeyAsNormalised = new HashMap<>();

        @Override
        public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {

                try {
                        Map<String, String> csvRow = newExchange.getIn().getBody(Map.class);
                        Integer rowNumber = (Integer) newExchange.getProperty("CamelSplitIndex") + 1;

                        if (rowNumber == 1) {
                                System.out.println("3333333333333333333333333333333333333333333333333333333");

                                for (String key : csvRow.keySet()) {
                                        String normalizedKey = key.trim().toLowerCase().replace(" ", "");
                                        csvColumnSetKeyAsNormalised.put(normalizedKey, key);
                                }

                        }
                       
                        List<GstDistributionConfiguration> gstRates = Arrays.asList(
                                        mapper.convertValue(newExchange.getVariable("gstDistribution"),
                                                        GstDistributionConfiguration[].class));

                        util.gstGstRates(gstRates, csvRow, csvColumnSetKeyAsNormalised);

                        if (oldExchange == null) {
                                Data data = new Data();
                                Report report = new Report();
                                data.setTaxableComponent(
                                                stringToDouble(csvRow.get(
                                                                csvColumnSetKeyAsNormalised.get("taxablecomponent"))));// slfeData.getTaxableComponent()
                                data.setNonTaxableFareComponent(
                                                stringToDouble(csvRow.get(csvColumnSetKeyAsNormalised
                                                                .get("non-taxablefarecomponent"))));
                                // data.setNonTaxableFareComponent(slfeData.getNonTaxableFareComponent());/
                                data.setCgstAmount(stringToDouble(
                                                csvRow.get(csvColumnSetKeyAsNormalised.get("cgstamount"))));
                                // data.setCgstAmount(slfeData.getCgstAmount());
                                data.setIgstAmount(stringToDouble(
                                                csvRow.get(csvColumnSetKeyAsNormalised.get("igstamount"))));
                                // data.setIgstAmount(slfeData.getIgstAmount());
                                data.setUgstAmount(stringToDouble(
                                                csvRow.get(csvColumnSetKeyAsNormalised.get("ugstamount"))));
                                // data.setUgstAmount(slfeData.getUgstAmount());
                                data.setSgstAmount(stringToDouble(
                                                csvRow.get(csvColumnSetKeyAsNormalised.get("sgstamount"))));
                                // data.setSgstAmount(slfeData.getSgstAmount());
                                data.setCessAmount(stringToDouble(
                                                csvRow.get(csvColumnSetKeyAsNormalised.get("cessamount"))));
                                // data.setCessAmount(slfeData.getCessAmount());

                                data.setImportFileName(newExchange.getIn().getHeader("CamelFileName").toString());
                                data.setCreatedBy("user");
                                data.setPnr(1);
                                data.setTransactionDate(csvRow.get(csvColumnSetKeyAsNormalised.get("transactiondate")));
                                data.setRecords(1);

                                List<Data> datas = new ArrayList<>();
                                datas.add(data);
                                report.setData(datas);
                                report.setFileType("slfe");
                                newExchange.getIn().setBody(report);
                                return newExchange;
                        } else {

                                Report report = oldExchange.getIn().getBody(Report.class);
                                List<Data> datas = report.getData();

                                List<Data> filteredTransactions = datas.stream()
                                                .filter(transaction -> transaction.getTransactionDate()
                                                                .equals(csvRow.get(csvColumnSetKeyAsNormalised
                                                                                .get("transactiondate"))))
                                                .collect(Collectors.toList());
                                if (filteredTransactions.size() != 0 && filteredTransactions.size() == 1) {
                                        Data filteredTransaction = filteredTransactions.get(0);

                                        filteredTransaction.setTaxableComponent(
                                                        stringToDouble(csvRow.get(csvColumnSetKeyAsNormalised
                                                                        .get("taxablecomponent")))
                                                                        + filteredTransaction.getTaxableComponent());
                                        filteredTransaction.setNonTaxableFareComponent(
                                                        stringToDouble(csvRow.get(csvColumnSetKeyAsNormalised
                                                                        .get("non-taxablefarecomponent")))
                                                                        + filteredTransaction
                                                                                        .getNonTaxableFareComponent());
                                        filteredTransaction
                                                        .setCgstAmount(stringToDouble(csvRow.get(
                                                                        csvColumnSetKeyAsNormalised.get("cgstamount")))
                                                                        + filteredTransaction.getCgstAmount());
                                        filteredTransaction
                                                        .setIgstAmount(stringToDouble(csvRow.get(
                                                                        csvColumnSetKeyAsNormalised.get("igstamount")))
                                                                        + filteredTransaction.getIgstAmount());
                                        filteredTransaction
                                                        .setUgstAmount(stringToDouble(csvRow.get(
                                                                        csvColumnSetKeyAsNormalised.get("ugstamount")))
                                                                        + filteredTransaction.getUgstAmount());
                                        filteredTransaction
                                                        .setSgstAmount(stringToDouble(csvRow.get(
                                                                        csvColumnSetKeyAsNormalised.get("sgstamount")))
                                                                        + filteredTransaction.getSgstAmount());
                                        filteredTransaction
                                                        .setCessAmount(stringToDouble(csvRow.get(
                                                                        csvColumnSetKeyAsNormalised.get("cessamount")))
                                                                        + filteredTransaction.getCgstAmount());
                                        filteredTransaction.setRecords(1 + filteredTransaction.getRecords());
                                } else if (filteredTransactions.size() == 0) {
                                        Data data = new Data();
                                        data.setTaxableComponent(
                                                        stringToDouble(csvRow.get(csvColumnSetKeyAsNormalised
                                                                        .get("taxablecomponent"))));// slfeData.getTaxableComponent()
                                        data.setNonTaxableFareComponent(
                                                        stringToDouble(csvRow.get(csvColumnSetKeyAsNormalised
                                                                        .get("non-taxablefarecomponent"))));
                                        // data.setNonTaxableFareComponent(slfeData.getNonTaxableFareComponent());/
                                        data.setCgstAmount(stringToDouble(
                                                        csvRow.get(csvColumnSetKeyAsNormalised.get("cgstamount"))));
                                        // data.setCgstAmount(slfeData.getCgstAmount());
                                        data.setIgstAmount(stringToDouble(
                                                        csvRow.get(csvColumnSetKeyAsNormalised.get("igstamount"))));
                                        // data.setIgstAmount(slfeData.getIgstAmount());
                                        data.setUgstAmount(stringToDouble(
                                                        csvRow.get(csvColumnSetKeyAsNormalised.get("ugstamount"))));
                                        // data.setUgstAmount(slfeData.getUgstAmount());
                                        data.setSgstAmount(stringToDouble(
                                                        csvRow.get(csvColumnSetKeyAsNormalised.get("sgstamount"))));
                                        // data.setSgstAmount(slfeData.getSgstAmount());
                                        data.setCessAmount(stringToDouble(
                                                        csvRow.get(csvColumnSetKeyAsNormalised.get("cessamount"))));
                                        // data.setCessAmount(slfeData.getCessAmount());

                                        data.setImportFileName(
                                                        newExchange.getIn().getHeader("CamelFileName").toString());
                                        data.setCreatedBy("user");
                                        data.setPnr(1);
                                        data.setTransactionDate(
                                                        csvRow.get(csvColumnSetKeyAsNormalised.get("transactiondate")));
                                        data.setRecords(1);
                                        datas.add(data);
                                } else {
                                        throw new RuntimeException(
                                                        "Exception during MyOrderStrategy aggregation more than one entry");
                                }
                                report.setData(datas);
                                newExchange.getIn().setBody(report);

                                return newExchange;
                        }
                } catch (Exception e) {
                        // Log the exception
                        e.printStackTrace();
                        System.err.println("Exception occurred during MyOrderStrategy aggregation: " + e.getMessage());
                        throw new RuntimeException("Exception during MyOrderStrategy aggregation", e);
                }
        }

        public double stringToDouble(String value) {
                value = (value == null || value.isEmpty()) ? "0.0" : value;
                return Double.parseDouble(value);
        }

}
