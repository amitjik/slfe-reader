package in.indigo.aggregationStrategy;

import in.indigo.entity.InvGLExtractdwh;
import in.indigo.util.RepositoryUtil;
import in.indigo.util.ResourceUtil;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;




@Singleton
public class GlOrderStrategy implements AggregationStrategy {

    @Inject
    ResourceUtil util;

    @Inject
    RepositoryUtil repositoryUtil;

    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {

        try {
            if (oldExchange == null) {
                Map<String, List<InvGLExtractdwh>> tranDateBucket = new HashMap<>();

                InvGLExtractdwh glExtractdwh = newExchange.getIn().getBody(InvGLExtractdwh.class);

                glExtractdwh.setIndex((Integer) newExchange.getProperty("CamelSplitIndex") + 1);
                glExtractdwh.setFileName(newExchange.getIn().getHeader("CamelFileName").toString());
                System.out.println("Gl--->"+glExtractdwh);
                repositoryUtil.dumpGl(glExtractdwh);


                List<InvGLExtractdwh> glExtractdwhs = new ArrayList<>();
                glExtractdwhs.add(glExtractdwh);
                System.out.println(glExtractdwh.getCreationDate());
                tranDateBucket.put(glExtractdwh.getCreationDate(), glExtractdwhs);
                newExchange.getIn().setBody(tranDateBucket);
                return newExchange;

            }else {
                Map<String, List<InvGLExtractdwh>> tranDateBucket = new HashMap<>();
                tranDateBucket = (Map<String, List<InvGLExtractdwh>>) oldExchange.getIn().getBody(Map.class);

                List<InvGLExtractdwh> invGLExtractdwh = new ArrayList<>();
                InvGLExtractdwh invGLExtractdwhs = newExchange.getIn().getBody(InvGLExtractdwh.class);

                invGLExtractdwhs.setIndex((Integer) newExchange.getProperty("CamelSplitIndex") + 1);
                invGLExtractdwhs.setFileName(newExchange.getIn().getHeader("CamelFileName").toString());
                repositoryUtil.dumpGl(invGLExtractdwhs);


                if (tranDateBucket.containsKey(invGLExtractdwhs.getCreationDate())) {
                    invGLExtractdwh.addAll(tranDateBucket.get(invGLExtractdwhs.getCreationDate()));
                    invGLExtractdwh.add(invGLExtractdwhs);
                    tranDateBucket.put(invGLExtractdwhs.getCreationDate(), invGLExtractdwh);
                } else {
                    invGLExtractdwh.add(invGLExtractdwhs);
                    tranDateBucket.put(invGLExtractdwhs.getCreationDate(), invGLExtractdwh);

                }

                newExchange.getIn().setBody(tranDateBucket);
                return newExchange;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Exception occurred during GlOrderStrategy aggregation: " + e.getMessage());
            throw new RuntimeException("Exception during GlOrderStrategy aggregation", e);
        }
    }
}
