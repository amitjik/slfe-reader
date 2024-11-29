package in.indigo.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import in.indigo.resource.Data;
import in.indigo.resource.Report;
import in.indigo.entity.InvSkyExtract;

public class ReportProcessor implements Processor {

        @Override
        public void process(Exchange exchange) throws Exception {
                Map<String, List<InvSkyExtract>> tranDateBucket = new HashMap<>();
                try {
                        tranDateBucket = exchange.getIn().getBody(Map.class);
                } catch (Exception e) {
                        // Log the exception
                        System.err.println("Exception occurred during body exchange: " + e.getMessage());
                        e.printStackTrace();
                        throw new RuntimeException("Exception during  body exchange", e);
                }
                Report report = new Report();
                List<Data> datas = new ArrayList<>();
                tranDateBucket.forEach((key, value) -> {
                        Data data = new Data();
                        double taxableFareComponent = value.stream()
                                        .mapToDouble(InvSkyExtract::getTaxableFareComponent)
                                        .sum();
                        double nonTaxableFareComponent = value.stream()
                                        .mapToDouble(InvSkyExtract::getNonTaxableFareComponent)
                                        .sum();
                        double cgstAmount = value.stream()
                                        .mapToDouble(InvSkyExtract::getCgstAmount)
                                        .sum();
                        double igstAmount = value.stream()
                                        .mapToDouble(InvSkyExtract::getIgstAmount)
                                        .sum();
                        double ugstAmount = value.stream()
                                        .mapToDouble(InvSkyExtract::getUgstAmount)
                                        .sum();
                        double sgstAmount = value.stream()
                                        .mapToDouble(InvSkyExtract::getSgstAmount)
                                        .sum();
                        double cessAmount = value.stream()
                                        .mapToDouble(InvSkyExtract::getCessAmount)
                                        .sum();
                        long recordCount = value.stream()
                                        .count();
                        data.setCessAmount(cessAmount);
                        data.setCgstAmount(cgstAmount);
                        data.setCreatedBy("user");
                        data.setIgstAmount(igstAmount);
                        data.setImportFileName(exchange.getIn().getHeader("CamelFileName").toString());
                        data.setNonTaxableFareComponent(nonTaxableFareComponent);
                        data.setPnr(value.size());
                        data.setRecords(recordCount);
                        data.setSgstAmount(sgstAmount);
                        data.setTaxableComponent(taxableFareComponent);
                        data.setTransactionDate(key);
                        data.setUgstAmount(ugstAmount);
                        datas.add(data);

                });
                report.setData(datas);
                report.setFileType("slfe");
                exchange.getIn().setBody(report);

        }

}
