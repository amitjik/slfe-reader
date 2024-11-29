package in.indigo.route;

import in.indigo.aggregationStrategy.GlOrderStrategy;
import in.indigo.entity.InvGLExtractdwh;
import in.indigo.processor.GlProcessor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http.HttpMethods;
import org.apache.camel.dataformat.bindy.csv.BindyCsvDataFormat;
import org.apache.camel.model.dataformat.JsonLibrary;

import java.util.Map;

@ApplicationScoped
public class GlRoute extends RouteBuilder {

    @Inject
    GlOrderStrategy glOrderStrategy;

    @Override
    public void configure() throws Exception {

        BindyCsvDataFormat bindy = new BindyCsvDataFormat(InvGLExtractdwh.class);
        bindy.setLocale("default");

        onException(Exception.class)
                .log("Start of Handling general GlRoute exception")
                .log("Exception caught: ${exception.message}")
                .handled(true)
                .log("End of Handling general GlRoute exception")
                .end();

        from("file://{{gl-file-path}}?delete=true")
                .id("route8")
                .log("Raw file content: ${body}")
                .unmarshal(bindy)
                .split(body(), glOrderStrategy)
                // .split().body().aggregationStrategy(new GlOrderStrategy())
                .log("my split")
                .end()
                .log(" body ------> ${body}")
                .to("direct:glreport")
                .end();

        from("direct:glreport")
                .id("route9")
                .process(new GlProcessor())
                .log(" body ------> ${body}")
                .marshal().json(JsonLibrary.Jackson)
                .unmarshal().json(Map.class)
                .setBody(exchange -> exchange.getIn().getBody(Map.class).get("glData"))
                .marshal().json(JsonLibrary.Jackson)
                .convertBodyTo(String.class)
                .log(" final body ------> ${body}")
                .removeHeaders("*")
                .setHeader(Exchange.CONTENT_TYPE, constant(MediaType.APPLICATION_JSON))
                .setHeader(Exchange.HTTP_METHOD, HttpMethods.POST)
                .to("{{insertGlUrl}}")
                // .to("file://data/report?fileName=report.json")
                .log("counter generated");

    }
}
