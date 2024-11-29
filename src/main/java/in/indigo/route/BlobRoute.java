package in.indigo.route;

import com.azure.core.util.polling.SyncPoller;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.monitorjbl.xlsx.StreamingReader;
import com.azure.storage.blob.models.BlobItem;

import in.indigo.util.RepositoryUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.camel.builder.RouteBuilder;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import in.indigo.aggregationStrategy.MyOrderStrategy;

import org.eclipse.microprofile.config.inject.ConfigProperty;

@Slf4j
@ApplicationScoped
public class BlobRoute extends RouteBuilder {

    @Inject
    MyOrderStrategy myOrderStrategy;

    @Inject
    RepositoryUtil repositoryUtil;

    @ConfigProperty(name = "BLOB_URL")
    private String blobUrl;

    @ConfigProperty(name = "BLOB_ACCOUNT_NAME")
    private String accountName;

    @ConfigProperty(name = "BLOB_PROCESSING_CONTAINER")
    private String processingContainer;

    @ConfigProperty(name="BLOB_ARCHIVE_CONTAINER")
    private String archiveContainer;

    @Override
    public void configure() throws Exception {

        String uri = String.format(blobUrl, accountName);

        ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                .clientId("98f0a50f-c434-4c59-813b-cc216bec3df7")
                .clientSecret("KbY8Q~NCj1Zlr.Nq8Vi_ns5t23KV1oNgXJinEc2u")
                .tenantId("73f2e714-a32e-4697-9449-dffe1df8a5d5")
                .build();

        BlobServiceClient client = new BlobServiceClientBuilder()
                .endpoint(uri)
                .credential(credential)
                .buildClient();


        // Bind the client to the Camel context
        getContext().getRegistry().bind("client", client);

        onException(Exception.class)
                .log("Exception caught: ${exception.message}")
                .handled(true)
                .log("Handling BlobRoute general exception")
                .end();

        from("azure-storage-blob://sagstinvoicedev/paxinv-ncs-processing?operation=listBlobs&serviceClient=#client")
                .id("route1")
                .log("list blob : ${header.CamelAzureStorageBlobBlobName}")
                .process(exchange -> {
                    String blobName = exchange.getIn().getHeader("CamelAzureStorageBlobBlobName", String.class);
                    exchange.getIn().setHeader("blobName", blobName);

                    System.out.println("Blob name: " + blobName);
                })
                .to("direct:processBlob");

        from("direct:processBlob")
                .id("route2")
                .toD("azure-storage-blob://sagstinvoicedev/paxinv-ncs-processing?blobName=${header.blobName}&operation=getBlob&serviceClient=#client")
                .log("Reading data from file: ${header.blobName}")
                .process(exchange -> {
                    long start = System.currentTimeMillis();
                    try (
                            InputStream is = exchange.getIn().getBody(InputStream.class);

                            Workbook workbook = StreamingReader.builder()
                                    .rowCacheSize(100)
                                    .bufferSize(4096)
                                    .open(is)) {
                        Sheet sheet = workbook.getSheetAt(0);
                        long count = 0;
                        StringBuilder pipeSeparatedBuilder = new StringBuilder();
                        DataFormatter dataFormatter = new DataFormatter();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("M/d/yyyy");

                        for (Row row : sheet) {
                            count++;
                            int lastCellNum = row.getLastCellNum();
                            for (int cellNum = 0; cellNum < lastCellNum; cellNum++) {
                                Cell cell = row.getCell(cellNum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);

                                String cellValue;
                                if (cell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
                                    cellValue = dateFormat.format(cell.getDateCellValue());
                                } else {
                                    cellValue = dataFormatter.formatCellValue(cell);
                                }
                                pipeSeparatedBuilder.append(cellValue).append("|");
                            }
                            pipeSeparatedBuilder.setLength(pipeSeparatedBuilder.length() - 1);
                            pipeSeparatedBuilder.append("\n");
                        }
                        exchange.getIn().setBody(pipeSeparatedBuilder.toString());

                        String blobName = exchange.getIn().getHeader("CamelAzureStorageBlobBlobName", String.class);
                        exchange.getIn().setHeader("blobName", blobName);
                    }
                })
                .choice()
                .when(exchange -> {
                    String blobName = exchange.getIn().getHeader("blobName", String.class);

                    System.out.println("Blob----> "+blobName);
                    return blobName.startsWith("slfe") || blobName.startsWith("SLFE");
                })
                .to("file://{{slfe-file-path}}")
                .endChoice()
                .when(exchange -> {
                    String blobName = exchange.getIn().getHeader("blobName", String.class);
                    return blobName.startsWith("gl") || blobName.startsWith("GL");
                })
                .to("file://{{gl-file-path}}")
                .endChoice()
                .otherwise()
                .throwException(RuntimeException.class, "File name not valid")
                .endChoice()
                .end()
                .process(exchange -> {

                    client.getBlobContainerClient(processingContainer)
                            .getBlobClient(exchange.getIn().getHeader("blobName",String.class))
                            .delete();

//                    List<String> blobs = client.getBlobContainerClient(processingContainer).listBlobs()
//                            .stream()
//                            .map(BlobItem::getName)
//                            .collect(Collectors.toList());

//                    for(String blobName : blobs){
//                        client.getBlobContainerClient(processingContainer)
//                                .getBlobClient(blobName)
//                                .delete();
//                    }

                });

    }
}
