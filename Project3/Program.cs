using System;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SQSMessageProcessor;

namespace AWSLambda
{
    class Program
    {
        static void Main(string[] args)
        {
            string folderName = "PatientsFiles";  // Specify the folder name
            string filePath = Path.Combine(Environment.CurrentDirectory, folderName);  // Combine current directory with folder name
            string fileExtension = "*.xml";  // Specify the file extension pattern
  
            // Get all files with the specified extension in the folder
            string[] files = Directory.GetFiles(filePath, fileExtension);
            Console.WriteLine("uploading files...");


            string bucketName = "cloud-hybrid-project2";
            string endpointURL = "https://s3.amazonaws.com";
            // Replace with the URL of your S3 endpoint
            string accessKeyId = "";
            string secretAccessKey = "";
            //upload the file
            try
            {
                
                // loop all files uploadable to the bucket of type .xml
                foreach (string file in files)
                {
                   UploadFile(file, bucketName, endpointURL, accessKeyId, secretAccessKey);
                }
            }
            catch (AmazonS3Exception e)
            {

                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                //external error besides the upload process
                Console.WriteLine("Unknown Error encountered on server. Message:'{0}' ", e.Message);
            }
            // create a sqs service to create a dowload queue;
            Thread.Sleep(2000);
            var hostBuilder = new HostBuilder()
                .ConfigureServices((hostContext, services) =>
                {
 
                    services.AddHostedService<MessageProcessor>();
                });

            hostBuilder.Build().Run();



        }

        static void UploadFile(string filePath, string bucketName, string endpointURL, string accessKeyId, string secretAccessKey)
        {
            var credentials = new Amazon.Runtime.BasicAWSCredentials(accessKeyId, secretAccessKey);
            var config = new AmazonS3Config
            {
                RegionEndpoint = RegionEndpoint.USEast1,
                ServiceURL = endpointURL // Add the endpoint URL to the S3 config
            };

            using (var client = new AmazonS3Client(credentials, config))
            {
                using (var transferUtility = new TransferUtility(client))
                {
                    string objectKey = Path.GetFileName(filePath);

                    string fileType = Path.GetExtension(filePath).ToLower();
                    if (fileType != ".json" && fileType != ".xml")
                    {
                        Console.WriteLine("Invalid file type. Only JSON and XML files are accepted.");
                        return;
                    }

                    var uploadRequest = new TransferUtilityUploadRequest
                    {
                        BucketName = bucketName,
                        FilePath = filePath,
                        Key = objectKey
                    };

                    transferUtility.Upload(uploadRequest);

                    Console.WriteLine(objectKey + " uploaded to S3 successfully.");
                }
            }
        }
    }
}
