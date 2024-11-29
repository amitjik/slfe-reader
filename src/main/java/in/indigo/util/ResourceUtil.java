package in.indigo.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.camel.ProducerTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import in.indigo.entity.GstDistributionConfiguration;
import in.indigo.entity.InvSkyExtract;
import jakarta.ejb.Asynchronous;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class ResourceUtil {

        @Inject
        ProducerTemplate producerTemplate;

        @Inject
        ObjectMapper mapper;

        public void gstGstRates(List<GstDistributionConfiguration> gdbc, Map<String, String> invSkyExtract,
                        Map<String, String> keys) {

                String placeOfEmbarkation = invSkyExtract.get(keys.get("placeofembarkation"));
                log.info("place of embarkation:" + placeOfEmbarkation);

                if (placeOfEmbarkation != null && !placeOfEmbarkation.toLowerCase().equals("null")) {
                        List<GstDistributionConfiguration> filteredList = gdbc.stream()
                                        .filter(g -> placeOfEmbarkation
                                                        .equals(g.getPlaceOfEmbarkation()))
                                        .collect(Collectors.toList());

                        if (filteredList.size() > 0) {
                                gstCalculationPOENotNull(invSkyExtract, filteredList.get(0), keys);
                        }
                }

                gstCalculationPOENull(invSkyExtract, keys);

        }

        public void gstCalculationPOENotNull(Map<String, String> invSkyExtract, GstDistributionConfiguration gdbc,
                        Map<String, String> keys) {

                invSkyExtract.put(keys.get("cgstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("cgstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("gstamount")))
                                                                * gdbc.getCgst() / 100));

                invSkyExtract.put(keys.get("sgstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("sgstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("gstamount")))
                                                                * gdbc.getSgst() / 100));

                // invSkyExtract
                // .setSgstAmount((invSkyExtract.getSgstAmount()
                // + (invSkyExtract.getGstAmount() * gdbc.getSgst()) / 100));

                invSkyExtract.put(keys.get("ugstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("ugstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("gstamount")))
                                                                * gdbc.getUgst() / 100));

                // invSkyExtract
                // .setUgstAmount((invSkyExtract.getUgstAmount()
                // + (invSkyExtract.getGstAmount() * gdbc.getUgst()) / 100));

                invSkyExtract.put(keys.get("localcgstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("localcgstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("localgstamount")))
                                                                * gdbc.getCgst() / 100));
                // invSkyExtract.setLocalCGSTAmount(
                // (invSkyExtract.getLocalCGSTAmount()
                // + (invSkyExtract.getLocalGSTAmount() * gdbc.getCgst()) / 100));

                invSkyExtract.put(keys.get("localsgstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("localsgstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("localgstamount")))
                                                                * gdbc.getSgst() / 100));

                // invSkyExtract.setLocalSGSTAmount(
                // (invSkyExtract.getLocalSGSTAmount()
                // + (invSkyExtract.getLocalGSTAmount() * gdbc.getSgst()) / 100));

                invSkyExtract.put(keys.get("localugstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("localugstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("localgstamount")))
                                                                * gdbc.getUgst() / 100));

                // invSkyExtract.setLocalUGSTAmount(
                // (invSkyExtract.getLocalUGSTAmount()
                // + (invSkyExtract.getLocalGSTAmount() * gdbc.getUgst()) / 100));

                // return invSkyExtract;
        }

        public void gstCalculationPOENull(Map<String, String> invSkyExtract,
                        Map<String, String> keys) {
                             
                invSkyExtract.put(keys.get("cgstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("cgstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("gstamount")))
                                                                / 2));

                invSkyExtract.put(keys.get("sgstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("sgstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("gstamount")))
                                                                / 2));

                // invSkyExtract.setCgstAmount((invSkyExtract.getCgstAmount() +
                // invSkyExtract.getGstAmount() / 2));

                // invSkyExtract.setSgstAmount((invSkyExtract.getSgstAmount() +
                // invSkyExtract.getGstAmount() / 2));

                invSkyExtract.put(keys.get("localcgstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("localcgstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("localgstamount")))
                                                                / 2));
                // invSkyExtract.setLocalCGSTAmount(
                // (invSkyExtract.getLocalCGSTAmount()
                // + (invSkyExtract.getLocalGSTAmount() * gdbc.getCgst()) / 100));

                invSkyExtract.put(keys.get("localsgstamount"),
                                String.valueOf(stringToDouble(invSkyExtract.get(keys.get("localsgstamount")))
                                                + stringToDouble(invSkyExtract.get(keys.get("localgstamount")))
                                                                / 2));

                // invSkyExtract.setLocalCGSTAmount(
                // (invSkyExtract.getLocalCGSTAmount() + invSkyExtract.getLocalGSTAmount() /
                // 2));

                // invSkyExtract.setLocalSGSTAmount(
                // (invSkyExtract.getLocalSGSTAmount() + invSkyExtract.getLocalGSTAmount() /
                // 2));

                // return invSkyExtract;


        }

        @Asynchronous
        public void callAnotherRouteAsync(List<InvSkyExtract> slfeData) {
                // Send the message to another Camel route asynchronously using seda
                List<InvSkyExtract> newBdy = new ArrayList<>();
                try {
                        String body = mapper.writeValueAsString(slfeData);
                        newBdy = Arrays.asList(mapper.readValue(body, InvSkyExtract[].class));
                } catch (JsonProcessingException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }
                producerTemplate.sendBody("seda:anotherRoute", newBdy);

                // /slfeData.clear();
        }

        public double stringToDouble(String value) {
                value = (value == null || value.isEmpty()) ? "0.0" : value;
                return Double.parseDouble(value);
        }
}
