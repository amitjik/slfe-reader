package in.indigo.aggregationStrategy;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;

import in.indigo.entity.InvSkyExtract;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SlfeAggregator implements AggregationStrategy{

    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        // TODO Auto-generated method stub
try{
        InvSkyExtract newBody = newExchange.getIn().getBody(InvSkyExtract.class);
        if(oldExchange == null){
            List<InvSkyExtract> newBodyList = new ArrayList<>();
            newBodyList.add(newBody);
            newExchange.getIn().setBody(newBodyList);
        }else{
            List<InvSkyExtract> oldBody =  oldExchange.getIn().getBody(List.class);
            oldBody.add(newBody);
             newExchange.getIn().setBody(oldBody);
           
        }}  catch (Exception e) {
            // Log the exception
            System.err.println("Exception occurred during NewStrategy aggregation: " + e.getMessage());
            e.printStackTrace();

            // Rethrow the exception to be handled by Camel or other downstream processors
            throw new RuntimeException("Exception during NewStrategy aggregation", e);
        }
        return newExchange;
    }
    
}
