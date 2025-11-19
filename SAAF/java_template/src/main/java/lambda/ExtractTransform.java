package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Scanner;

public class ExtractTransform implements RequestHandler<Request, HashMap<String, Object>> {
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();;

        String bucketName = request.getBucketName();
        String fileName = request.getFilename();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));

        InputStream objectData = s3Object.getObjectContent();

        Scanner scanner = new Scanner(objectData);

        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
