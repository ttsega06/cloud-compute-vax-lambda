using System;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;

namespace Upload
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Please provide the file path as a command-line argument.");
                return;
            }

            string filePath = args[0];
            string bucketName = "vaccinebucket1";
            string endpointURL = "https://s3.amazonaws.com";
            // Replace with the URL of your S3 endpoint
            string accessKeyId = "";
            string secretAccessKey = "";

            try
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

                        // Set the "type" tag based on the fileType argument
                        uploadRequest.TagSet = new System.Collections.Generic.List<Amazon.S3.Model.Tag>
                        {
                            new Amazon.S3.Model.Tag { Key = "type", Value = fileType }
                        };

                        transferUtility.Upload(uploadRequest);

                        Console.WriteLine("File uploaded to S3 successfully.");
                    }
                }
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
        }
    }
}
