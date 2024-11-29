package in.indigo.route;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import in.indigo.aggregationStrategy.MyOrderStrategy;
import in.indigo.aggregationStrategy.SlfeAggregator;
import in.indigo.entity.InvSkyExtract;
import in.indigo.processor.CsvProcessor;
import in.indigo.util.RepositoryUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DBInsertRoute extends RouteBuilder {

    @Inject
    MyOrderStrategy myOrderStrategy;

    @Inject
    RepositoryUtil repositoryUtil;

    @Inject
    CsvProcessor csvProcessor;

    @Inject
    SlfeAggregator slfeAggregator;

    @ConfigProperty(name = "pnr-dump-file-path-after-dbinsert")
    String destinationPath;

    @ConfigProperty(name = "pnr-dump-file-path-after-error")
    String errorDestinationPath;

    @Override
    public void configure() throws Exception {

        CsvDataFormat csv = new CsvDataFormat();
        csv.setDelimiter('|');
        csv.setUseMaps(true); // Use maps so that we can dynamically handle headers

        onException(Exception.class)
                .log("fail to stor data into db for pnr: ${header.CamelFileName}")
                .log("Exception caught: ${exception.message}")
                .handled(true)
                .log("End of Handling general DBInsertRoute exception")
                .end();

        from("file://{{pnr-dump-file-path}}?recursive=true&delete=true&doneFileName=Done")
                .id("route6")
                .log("start reading file from folder based on pnr:${header.CamelFileName}")
                .setProperty("rawBody", simple("${body}"))
                .unmarshal(csv)
                .to("direct:insertdb")
                .log("end reading file from folder based on pnr:${header.CamelFileName}");

        from("direct:insertdb")
                .id("route7")
                .split(body(), slfeAggregator).stopOnException()
                .process(csvProcessor)
                .end()
                .doTry() // Try block for executing the main process
                .process(e -> {
                    List<InvSkyExtract> slfeData = Arrays
                            .asList(e.getIn().getBody(InvSkyExtract[].class));
                    repositoryUtil.dumpBatch(slfeData);
                })
                .doCatch(Exception.class) // Catch block for handling errors
                .log("Error occurred during processing: ${exception.message}")
                .setProperty("hasError", constant(true))
                .log("Fallback process executed")
                .doFinally() // Finally block (optional) - executed regardless of success or failure
                .process(exchange -> {
                    String str1 = destinationPath + "/"
                            + exchange.getIn().getHeader("CamelFileName");
                    File file = new File(str1);
                    if (!file.exists()) {
                        // File doesn't exist, create it
                        exchange.getIn().setHeader("hasFile", false);
                    } else {
                        exchange.getIn().setHeader("hasFile", true);
                    }
                })
                .setBody(simple("${exchangeProperty.rawBody}"))
                .choice()
                // Check if an error occurred and only move the file if no error was set
                .when(simple("${exchangeProperty.hasError} == null"))
                .choice()
                .when(simple("${header.hasFile}"))
                .unmarshal(csv)
                .marshal(csv)
                .toD("file://{{pnr-dump-file-path-after-dbinsert}}?fileName=${header.CamelFileName}&fileExist=Append")
                .otherwise()
                .toD("file://{{pnr-dump-file-path-after-dbinsert}}?fileName=${header.CamelFileName}")
                .endChoice()
                .otherwise()
                .process(exchange -> {
                    String str1 = errorDestinationPath + "/"
                            + exchange.getIn().getHeader("CamelFileName");
                    File file = new File(str1);
                    if (!file.exists()) {
                        // File doesn't exist, create it
                        exchange.getIn().setHeader("hasErrorFile", false);
                    } else {
                        exchange.getIn().setHeader("hasErrorFile", true);
                    }
                })
                // .toD("file://${header.CamelFileParent}?fileName=${header.CamelFileNameOnly}&fileExist=Override")
                .log("Error occurred, keeping the file in the error folder.")
                .choice()
                .when(simple("${header.hasErrorFile}"))
                .unmarshal(csv)
                .marshal(csv)
                .toD("file://{{pnr-dump-file-path-after-error}}?fileName=${header.CamelFileName}&fileExist=Append")
                .log("File moved successfully after error when file exist: ${header.CamelFileName}")
                .otherwise()
                .toD("file://{{pnr-dump-file-path-after-error}}?fileName=${header.CamelFileName}")
                .log("File moved successfully after error when file not exist ${header.CamelFileName}")
                .endChoice()
                .endChoice()
                .end()
                .end();
    }

}
