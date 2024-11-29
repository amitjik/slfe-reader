package in.indigo.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.ObjectMapper;

import in.indigo.entity.InvSkyExtract;
import in.indigo.util.CustomObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;


@ApplicationScoped
public class PnrProcessor implements Processor {
    ObjectMapper customMapper = CustomObjectMapper.createCustomObjectMapper();

    @ConfigProperty(name = "pnr-dump-file-path")
    String destinationPath;

    @Override
    public void process(Exchange exchange) throws Exception {
       InvSkyExtract invSkyExtract = exchange.getIn()
                .getBody(InvSkyExtract.class);

        String transactionDate = invSkyExtract.getTransactionDate() .toString().replace("/", "_");

        String directoryPath = destinationPath +"/"+ transactionDate;

        exchange.getIn().setHeader("directoryPath", directoryPath);
      
    }

}
