package in.indigo.processor;

import in.indigo.entity.InvGLExtractdwh;
import in.indigo.resource.GlData;
import in.indigo.resource.Report;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.*;

public class GlProcessor implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        Map<String, List<InvGLExtractdwh>> tranDateBucket = new HashMap<>();
        try {
            tranDateBucket = exchange.getIn().getBody(Map.class);
        } catch (Exception e) {
            System.err.println("Exception occurred during body exchange: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Exception during body exchange", e);
        }

        Report report = new Report();
        List<GlData> datas = new ArrayList<>();

        tranDateBucket.forEach((key, value) -> {
            GlData aggregatedData = new GlData();
            double totalCgstAmount = 0.0;
            double totalSgstAmount = 0.0;
            double totalIgstAmount = 0.0;
            double totalGstAmount = 0.0;
            long totalRecords = 0;

            Set<String> uniquePnrs = new HashSet<>();

            for (InvGLExtractdwh invGLExtractdwh : value) {
                String referenceCode = invGLExtractdwh.getReferenceCode();
                String hostAmountStr = invGLExtractdwh.getHostAmount();

                Double hostAmount = Double.parseDouble(hostAmountStr);

                if (referenceCode.contains("IGST")) {
                    totalIgstAmount += hostAmount;
                } else if (referenceCode.contains("GST")) {
                    totalGstAmount += hostAmount;
//                    totalCgstAmount += hostAmount / 2;
//                    totalSgstAmount += hostAmount / 2;
                }

//                totalGstAmount += hostAmount;
                totalRecords++;

                uniquePnrs.add(invGLExtractdwh.getLedgerkey());
            }

            aggregatedData.setCgstAmount(totalCgstAmount);
            aggregatedData.setSgstAmount(totalSgstAmount);
            aggregatedData.setIgstAmount(totalIgstAmount);
            aggregatedData.setGstAmount(totalGstAmount);
            aggregatedData.setRecords(totalRecords);
            aggregatedData.setTransactionDate(key);
            aggregatedData.setImportFileName(value.get(0).getFileName());
            aggregatedData.setPnrCount(uniquePnrs.size());

            datas.add(aggregatedData);
        });

        report.setGlData(datas);
        report.setFileType("gl");
        exchange.getIn().setBody(report);
    }

}
