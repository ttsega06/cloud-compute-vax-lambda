using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace SQSMessageProcessor
{
    public class MessageProcessor : BackgroundService
    {
        string _sourceQueueUrl = "";
        string _destinationQueueUrl = "";
        string accessKeyId = "";
        string _insuranceDatabasePath = "";
        string secretAccessKey = "";



        IAmazonSQS _sqsClient;

        public MessageProcessor()
        {
            var credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
            _sqsClient = new AmazonSQSClient(credentials, Amazon.RegionEndpoint.USEast1);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            string folderName = "InsuranceDatabase";  // Specify the folder name
            string filePath = Path.Combine(Environment.CurrentDirectory, folderName);  // Combine current directory with folder name
            string fileExtension = "*.xml";  // Specify the file extension pattern

            // Get all files with the specified extension in the folder
            string[] files = Directory.GetFiles(filePath, fileExtension);
             _insuranceDatabasePath = files[0];

            

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var receiveRequest = new ReceiveMessageRequest()
                    {
                        QueueUrl = _sourceQueueUrl,
                    };

                    var receiveResponse = await _sqsClient.ReceiveMessageAsync(receiveRequest, stoppingToken);

                    foreach (var message in receiveResponse.Messages)
                    {
                        string patientId = "";
                        string name = "";

                        //to extract data from the downmessage queue
                        string idPattern = @"Patient ID: (\d+)";
                        string namePattern = @"Name: ([A-Za-z\s]+)";

                        Match idMatch = Regex.Match(message.Body.ToString(), idPattern);
                        Match nameMatch = Regex.Match(message.Body.ToString(), namePattern);

                        if (idMatch.Success && nameMatch.Success)
                        {
                             patientId = idMatch.Groups[1].Value;
                             name = nameMatch.Groups[1].Value;
                        }
                        else
                        {
                    
                        }




                       

                        // Check if patient ID exists in the XML database
                        Console.WriteLine("checking for patient " + patientId);
                        if (CheckPatientIdExists(patientId))
                        {
                            // Extract patient information from the XML database
                            var patientInfo = GetPatientInfo(patientId);

                            // Create a new message with the extracted information
                            var newMessage = $"Patient ID: {patientInfo.Id}, Policy Number: {patientInfo.PolicyNumber}, Policy Provider: {patientInfo.PolicyProvider}";

                            // Send the new message to the destination queue
                            var sendMessageRequest = new SendMessageRequest
                            {
                                QueueUrl = _destinationQueueUrl,
                                MessageBody = newMessage
                            };

                            await _sqsClient.SendMessageAsync(sendMessageRequest, stoppingToken);
                            Console.WriteLine(sendMessageRequest.MessageBody);
                        }
                        else
                        {
                            var newMessage = "Patient with ID " + patientId + " does not have medical insurance";
                            var sendMessageRequest = new SendMessageRequest
                            {
                                QueueUrl = _destinationQueueUrl,
                                MessageBody = newMessage
                            };
                            await _sqsClient.SendMessageAsync(sendMessageRequest, stoppingToken);

                            Console.WriteLine(  "Patient with ID "+ patientId + " does not have medical insurance");
                        }

                        // Delete the processed message from the source queue
                        var deleteRequest = new DeleteMessageRequest
                        {
                            QueueUrl = _sourceQueueUrl,
                            ReceiptHandle = message.ReceiptHandle
                        };

                        await _sqsClient.DeleteMessageAsync(deleteRequest, stoppingToken);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }

                // Delay between processing batches of messages
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        private bool CheckPatientIdExists(string patientId)
        {
            try
            {
                var xmlDoc = new XmlDocument();
                                                                                xmlDoc.Load(_insuranceDatabasePath);

                var patientNode = xmlDoc.SelectSingleNode($"//patient[@id='{patientId}']");
                return patientNode != null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error checking patient ID: " + ex.Message);
                return false;
            }
        }

        private PatientInfo GetPatientInfo(string patientId)
        {
            try
            {
                var xmlDoc = new XmlDocument();
                xmlDoc.Load(_insuranceDatabasePath);

                var patientNode = xmlDoc.SelectSingleNode($"//patient[@id='{patientId}']");
                if (patientNode != null)
                {
                    var id = patientNode.Attributes["id"].Value;
                    var policyNumber = patientNode.SelectSingleNode("policy").Attributes["policyNumber"].Value;
                    var policyProvider = patientNode.SelectSingleNode("policy/provider").InnerText;

                    return new PatientInfo
                    {
                        Id = id,
                        Name = GetPatientName(patientNode),
                        PolicyNumber = policyNumber,
                        PolicyProvider = policyProvider
                    };
                }
                else
                {
                    throw new Exception($"Patient with ID '{patientId}' does not exist in the database.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error getting patient information: " + ex.Message);
                return null;
            }
        }

        private string GetPatientName(XmlNode patientNode)
        {
            var nameNode = patientNode.SelectSingleNode("name");
            return nameNode != null ? nameNode.InnerText : string.Empty;
        }
    }
}

public class PatientInfo
{
    public string Id { get; set; }
    public string Name { get; set; }
    public string PolicyNumber { get; set; }
    public string PolicyProvider { get; set; }
}
