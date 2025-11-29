package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ExtractTransform implements RequestHandler<Request, HashMap<String, Object>> {
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        //Collect initial data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

//        LambdaLogger logger = context.getLogger();

        String bucketname = request.getBucketname();
        String filename = request.getFilename();
//        logger.log("Received bucketname:" + bucketname + " Received filename:" + filename);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));

        InputStream objectData = s3Object.getObjectContent();

        HashSet<Sale> sales = new HashSet<>();

        // Read sales data from CSV
        Scanner scanner = new Scanner(objectData);
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            // Skips column names
            if (line.startsWith("Region,Country,")) {
                continue;
            }
            Sale newSale = getSale(line);

            sales.add(newSale);
        }
        scanner.close();

        StringWriter sw = new StringWriter();
        // Add column names
        sw.append("Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit,Order Processing Time,Gross Margin\n");
        // Write sales data to new CSV
        for (Sale s : sales) {
            sw.append(s.toString()).append("\n");
        }

        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");

        AmazonS3 s3ClientOutput = AmazonS3ClientBuilder.standard().build();
        // Creates a new file Name;
        int dotIndex = filename.lastIndexOf(".");
        String newFileName = filename.substring(0, dotIndex) + "-et" + filename.substring(dotIndex);

        s3ClientOutput.putObject(bucketname, newFileName, is, meta);

        Response response = new Response();
        response.setValue("Bucket:" + bucketname + " filename:" + newFileName + " size:" + bytes.length);

        inspector.consumeResponse(response);

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private static Sale getSale(String line) {
        String[] data = line.split(",");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");

        return new Sale(
                data[0],
                data[1],
                data[2],
                data[3],
                data[4],
                LocalDate.parse(data[5], formatter),
                Integer.parseInt(data[6]),
                LocalDate.parse(data[7], formatter),
                Integer.parseInt(data[8]),
                new BigDecimal(data[9]),
                new BigDecimal(data[10]),
                new BigDecimal(data[11]),
                new BigDecimal(data[12]),
                new BigDecimal(data[13]));
    }
}
