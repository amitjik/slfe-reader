package in.indigo.route;

import java.util.List;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;

import in.indigo.entity.GstDistributionConfiguration;
import in.indigo.util.RepositoryUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class GstDistributionRoute extends RouteBuilder {

    @Inject
    RepositoryUtil repositoryUtil;

    @Override
    public void configure() throws Exception {

        onException(Exception.class)
                .log("Start of Handling general GstDistributionRoute exception")
                .log("Exception caught: ${exception.message}")
                .handled(true)
                .log("End of Handling general GstDistributionRoute exception")
                .end();

        from("timer://myTimer?repeatCount=1")
                .id("route11")
                .log("Camel route triggered on application startup!")
                .process(e -> {
                    List<GstDistributionConfiguration> gstConf = repositoryUtil.getGstRate();
                    e.getIn().setBody(gstConf);
                })

                .setVariable("global:gstDistributionConfiguration", simple("${body}"))
                .setVariable("global:success", simple("true"))
                .marshal().json(JsonLibrary.Jackson)
                .log("${body}");
    }

}
