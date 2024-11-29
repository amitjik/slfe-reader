package in.indigo.route;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import in.indigo.entity.PkgAudit;
import in.indigo.processor.CSVValidator;
import in.indigo.processor.ObjectToMapProcessor;
import in.indigo.resource.Data;
import in.indigo.resource.Report;
import in.indigo.util.CustomObjectMapper;
import in.indigo.util.RepositoryUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http.HttpMethods;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.apache.camel.model.dataformat.JsonLibrary;

import com.fasterxml.jackson.databind.ObjectMapper;

import in.indigo.aggregationStrategy.MyOrderStrategy;

import org.apache.commons.io.FileUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Slf4j
@ApplicationScoped
public class CsvRoute extends RouteBuilder {

        @Inject
        MyOrderStrategy myOrderStrategy;

        @Inject
        RepositoryUtil repositoryUtil;

        @Inject
        ObjectToMapProcessor objectToMapProcessor;

        @Inject
        CSVValidator csvValidator;

        @ConfigProperty(name = "pnr-dump-file-path")
        String pnrdumpfilepath;

        @Override
        public void configure() throws Exception {
                CsvDataFormat csv = new CsvDataFormat();
                csv.setDelimiter('|');
                csv.setUseMaps(true); // Use maps so that we can dynamically handle headers

                ObjectMapper customMapper = CustomObjectMapper.createCustomObjectMapper();
                JacksonDataFormat customJackson = new JacksonDataFormat();
                customJackson.setObjectMapper(customMapper);

                onException(Exception.class)
                                .log("Start of Handling general CsvRoute exception")
                                .log("Exception caught: ${exception.message}")
                                .setHeader("exceptionMessage", simple("Exception caught: ${exception.message}"))
                                .handled(true)
                                .process(exchange -> {
                                        PkgAudit pkgAudit = new PkgAudit();
                                        pkgAudit.setImportFileName(
                                                        exchange.getProperty("fileName").toString());
                                        pkgAudit.setRecords((Integer) exchange.getProperty("rowCount"));
                                        pkgAudit.setDescription(
                                                        exchange.getIn().getHeader("exceptionMessage", String.class));
                                        repositoryUtil.dumpPkgAudit(pkgAudit);
                                        Thread.sleep(5000);
                                        System.out.println("ddddddddddddddddddddddddddddddddddddddddddddd"
                                                        + pnrdumpfilepath);
                                        FileUtils.forceDelete(new File(pnrdumpfilepath));

                                })
                                .log("End of Handling general CsvRoute exception")
                                .end();

                from("file://{{slfe-file-path}}?delete=true&maxMessagesPerPoll=1")
                                .id("route3")
                                .log("Start of slfe file reading")
                                .unmarshal(csv)
                                .process(exchange -> {
                                        List<Map<String, String>> body = Arrays
                                                        .asList(exchange.getIn().getBody(Map[].class));
                                        log.info("number of record to the file: "
                                                        + exchange.getIn().getHeader("CamelFileName", String.class)
                                                        + " is: " + body.size());
                                        exchange.setProperty("rowCount", body.size());
                                        exchange.setProperty("fileName",
                                                        exchange.getIn().getHeader("CamelFileName", String.class));
                                })
                                .log("my global----------------->${variable.global:gstDistributionConfiguration}")
                                .setVariable("gstDistribution",
                                                simple("${variable.global:gstDistributionConfiguration}"))
                                .split(body(), myOrderStrategy).streaming().stopOnException()
                                .log("${exchangeProperty.CamelSplitIndex}")
                                .process(csvValidator)
                                .to("direct:checkFile")
                                .end()
                                // .marshal().json(JsonLibrary.Jackson)
                                .log("aggregation body : ${body}")
                                .process(exc -> {
                                        Report report = exc.getIn().getBody(Report.class);
                                        Set<String> distinctTransactionDates = report.getData().stream()
                                                        .map(Data::getTransactionDate) // Extract transaction dates
                                                        .collect(Collectors.toSet());
                                        exc.setProperty("distinctTransactionDates", distinctTransactionDates);
                                })
                                .to("direct:report")
                                .setBody(simple("${exchangeProperty.distinctTransactionDates}"))
                                .split().body()
                                .process(exc->{
                                        String transactionDate = exc.getIn().getBody(String.class).toString().replace("/","_");
                                        exc.getIn().setBody(transactionDate);
                                })
                                .log("tttttttttttttttttttttttttttttttttttt${body}")
                                .toD("file://{{pnr-dump-file-path}}/${body}?fileName=Done")
                                .end()
                                .log("End of slfe file reading")
                                .end();

                from("direct:checkFile")
                                // .log("Start of checkfile ${body}")
                                .id("route5")
                                .setProperty("oldBody", simple("${body}"))
                                .process(objectToMapProcessor)
                                .marshal(csv)
                                .log("start storing file into folder based on pnr")
                                .toD("file://${header.directoryPath}?noop=true&fileExist=Append")
                                .log("end storing file into folder based on pnr")
                                .setBody(simple("${exchangeProperty.oldBody}"))
                                .log("my final last body")
                                .end();

                from("direct:report")
                                .log("start of report route")
                                .id("route10")
                                // .process(new ReportProcessor())
                                .log(" body ------> ${body}")
                                .marshal().json(JsonLibrary.Jackson)
                                .unmarshal().json(Map.class)
                                .setBody(exchange -> exchange.getIn().getBody(Map.class).get("data"))
                                .marshal().json(JsonLibrary.Jackson)
                                .convertBodyTo(String.class)
                                .log(" final body ------> ${body}")
                                .removeHeaders("*")
                                .setHeader(Exchange.CONTENT_TYPE, constant(MediaType.APPLICATION_JSON))
                                .setHeader(Exchange.HTTP_METHOD, HttpMethods.POST)
                                .to("{{reportUrl}}")
                                // .to("file://data/report?fileName=report.json")
                                .log("counter generated");

        }
}
