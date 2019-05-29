package S3APP2;


import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.lifecycle.LifecycleAndOperator;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecycleTagPredicate;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;

/*
public class S3APP2 {

    public static void main(String[] args) throws IOException {
        String clientRegion = "us-east-1";
        String bucketName = "scd-joeguo-athena-case";
        String objectKey = "s3_access_log/2019-04-27-00-37-05-CC4FEA144417F2D3.txt";

        try {            
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
    
            // Set the presigned URL to expire after one hour.
            java.util.Date expiration = new java.util.Date();
            long expTimeMillis = expiration.getTime();
            expTimeMillis += 1000 * 60 * 60;
            expiration.setTime(expTimeMillis);

            // Generate the presigned URL.
            System.out.println("Generating pre-signed URL.");
            GeneratePresignedUrlRequest generatePresignedUrlRequest = 
                    new GeneratePresignedUrlRequest(bucketName, objectKey)
                    .withMethod(HttpMethod.GET)
                    .withExpiration(expiration);
            URL url = s3Client.generatePresignedUrl(generatePresignedUrlRequest);
    
            System.out.println("Pre-Signed URL: " + url.toString());
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }
}

*/

/*
public class S3APP2 {

    public static void main(String[] args) throws IOException {
        String clientRegion = "ap-southeast-2";
        String bucketName = "scd-joeguo-case";
        
        // Create a rule to archive objects with the "glacierobjects/" prefix to Glacier immediately.
        BucketLifecycleConfiguration.Rule rule1 = new BucketLifecycleConfiguration.Rule()
                .withId("Archive immediately rule")
                .withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("")))
                .addTransition(new Transition().withDays(0).withStorageClass(StorageClass.Glacier))
                .withStatus(BucketLifecycleConfiguration.ENABLED);

        // Create a rule to transition objects to the Standard-Infrequent Access storage class 
        // after 30 days, then to Glacier after 365 days. Amazon S3 will delete the objects after 3650 days.
        // The rule applies to all objects with the tag "archive" set to "true". 
        BucketLifecycleConfiguration.Rule rule2 = new BucketLifecycleConfiguration.Rule()
                .withId("Archive and then delete rule")
                .withFilter(new LifecycleFilter(new LifecycleTagPredicate(new Tag("archive", "true"))))
                .addTransition(new Transition().withDays(30).withStorageClass(StorageClass.StandardInfrequentAccess))
                .addTransition(new Transition().withDays(365).withStorageClass(StorageClass.Glacier))
                .withExpirationInDays(3650)
                .withStatus(BucketLifecycleConfiguration.ENABLED);

        // Add the rules to a new BucketLifecycleConfiguration.
        BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration()
                .withRules(Arrays.asList(rule1, rule2));

        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(clientRegion)
                    .build();

            // Save the configuration.
            s3Client.setBucketLifecycleConfiguration(bucketName, configuration);
    
            // Retrieve the configuration.
            configuration = s3Client.getBucketLifecycleConfiguration(bucketName);
    
            // Add a new rule with both a prefix predicate and a tag predicate.
            configuration.getRules().add(new BucketLifecycleConfiguration.Rule().withId("NewRule")
                    .withFilter(new LifecycleFilter(new LifecycleAndOperator(
                            Arrays.asList(new LifecyclePrefixPredicate("YearlyDocuments/"),
                                          new LifecycleTagPredicate(new Tag("expire_after", "ten_years"))))))
                    .withExpirationInDays(3650)
                    .withStatus(BucketLifecycleConfiguration.ENABLED));
    
            // Save the configuration.
            s3Client.setBucketLifecycleConfiguration(bucketName, configuration);
    
            // Retrieve the configuration.
            configuration = s3Client.getBucketLifecycleConfiguration(bucketName);
    
            // Verify that the configuration now has three rules.
            configuration = s3Client.getBucketLifecycleConfiguration(bucketName);
            System.out.println("Expected # of rules = 3; found: " + configuration.getRules().size());
    
            // Delete the configuration.
            //s3Client.deleteBucketLifecycleConfiguration(bucketName);
    
            // Verify that the configuration has been deleted by attempting to retrieve it.
            configuration = s3Client.getBucketLifecycleConfiguration(bucketName);
            String s = (configuration == null) ? "No configuration found." : "Configuration found.";
            System.out.println(s);
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }
}

*/


public class S3APP2
{
//    private static final Logger logger = LogManager.getLogger(S3APP1.class);
    private static AmazonS3 s3 ;
//= S3Client.builder().S3Configuration.builder().pathStyleAccessEnabled(true).build().build();
        //private static S3AdvancedConfiguration s3Configuration;

    public static void main( String[] args )
    {
        //System.out.println("Printing parameters");
        //for(int i=0;i<args.length;i++)
//              System.out.println(args[i]);
//      }

        System.out.println( "Hello World!" );

        String region = "ap-southeast-2";
        s3 = AmazonS3ClientBuilder.standard()
        		.withRegion(region)
                .build();

        String bucket = "guojing1217-public-access";
        String key = "/test_billing_report1/20190301-20190401/test_billing_report1-Manifest.json";
        //String key = "/testfile";
        //String key = "aaaa";
        //String key = "prefix1/test_billing_report2/20190301-20190401/test_billing_report2-Manifest.json";

        //HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();
        //HeadObjectResponse response = null;
        //Path path = Paths.get("/tmp/testfile");
        try {
            s3.getObject(bucket,key);
        	//PutObjectResponse putResponse = null;
            //putResponse = s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),RequestBody.fromByteBuffer(getRandomByteBuffer(10_000))                                                      );
            System.out.println( "No expception is throwed");
    } catch (AmazonServiceException  e) {
            System.out.println( "Error Code: " + e.getErrorMessage());
            //throw e;
    }
    //S3ResponseMetadata s3ResponseMetadata = s3.getCachedResponseMetadata(getObjectRequest);
}
private static ByteBuffer getRandomByteBuffer(int size){
    byte[] b = new byte[size];
    new Random().nextBytes(b);
    return ByteBuffer.wrap(b);
}
}
