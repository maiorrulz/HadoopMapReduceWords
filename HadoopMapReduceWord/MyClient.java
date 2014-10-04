package abc;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceAsync;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceAsyncClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
 


import java.util.Arrays;
import java.util.Date;
import java.util.List;


 

public class MyClient {
 
    public static void main(String[] args) throws IOException {
	
	    AWSCredentials credentials = null;
	    try {
	        credentials = new ProfileCredentialsProvider("default").getCredentials();
	    } catch (Exception e) {
	        throw new AmazonClientException(
	                "Cannot load the credentials from the credential profiles file. " +
	                "Please make sure that your credentials file is at the correct " +
	                "location (C:\\Users\\Vadim\\.aws\\credentials), and is in valid format.",
	                e);
	    }
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
		 
		HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
		    .withJar("s3n://maiorbucket2/gruzin/wordcount.jar") // This should be a full map reduce application.
		    .withArgs("s3n://elasticmapreduce/samples/wordcount/input", "s3n://maiorbucket2/gruzin/");
		 
		StepConfig stepConfig = new StepConfig()
		    .withName("stepname")
		    .withHadoopJarStep(hadoopJarStep)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		 
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		    .withInstanceCount(2)
		    .withMasterInstanceType(InstanceType.M1Medium .toString())
		    .withSlaveInstanceType(InstanceType.M1Medium .toString())
		    .withHadoopVersion("2.2.0")
		    .withKeepJobFlowAliveWhenNoSteps(false);
		 
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		    .withName("jobname")
		    .withInstances(instances)
		    .withSteps(stepConfig)
		    .withLogUri("s3n://maiorbucket2/gruzin/");
		 
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);
	}
}