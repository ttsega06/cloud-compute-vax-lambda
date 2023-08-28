using System;
using System.IO;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Npgsql;
using System.Text.Json;
using System.Xml.Linq;
using NpgsqlTypes;
using System.Globalization;
using System.Xml;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AWSLambda1
{
    public class Function
    {
        // The S3 client to use for accessing S3
        private readonly IAmazonS3 _s3Client;

        // The connection string to the PostgreSQL database
        //private readonly string _connectionString = "Host=...;Username=...;Password=...;Database=...";
        private readonly string _connectionString =
              "Host=database-1.caodvlrmlx03.eu-north-1.rds.amazonaws.com;" +
              "Username=postgres;" +
              "Password=postgres12;" +
              "Database=postgres";
        // The constructor that Lambda will invoke.
        public Function()
        {
            _s3Client = new AmazonS3Client();
        }

        // The method that Lambda will call when a file is uploaded to S3
        public async Task FunctionHandler(S3Event s3Event, ILambdaContext context)
        {
            try
            {
                // Get the S3 object key and bucket name from the event
                var s3EventRecord = s3Event.Records[0];
                LambdaLogger.Log($"S3 event record: {s3EventRecord}");
                var s3ObjectKey = s3EventRecord.S3.Object.Key;
                var s3BucketName = s3EventRecord.S3.Bucket.Name;
                LambdaLogger.Log($"Bucket Name: {s3BucketName}");
                LambdaLogger.Log($"The key: {s3ObjectKey}");

                // Get the type tag value from the S3 object
                var getTaggingRequest = new GetObjectTaggingRequest
                {
                    BucketName = s3BucketName,
                    Key = s3ObjectKey
                };
                var getTaggingResponse = await _s3Client.GetObjectTaggingAsync(getTaggingRequest);
                var typeTag = getTaggingResponse.Tagging.Find(t => t.Key == "type");
                //var typeValue = typeTag.Value;
                var typeValue = typeTag.Value.Replace(".", "");
                LambdaLogger.Log(typeValue);

                // Get the file content from the S3 object
                var getObjectRequest = new GetObjectRequest
                {
                    BucketName = s3BucketName,
                    Key = s3ObjectKey
                };

                LambdaLogger.Log($"The request: {getObjectRequest}");
                var getObjectResponse = await _s3Client.GetObjectAsync(getObjectRequest);
                using (var streamReader = new StreamReader(getObjectResponse.ResponseStream))
                {
                    var fileContent = streamReader.ReadToEnd();
                    LambdaLogger.Log($"File content: {fileContent}");

                    // Parse the file content based on the type value and extract vaccination data
                    int total, firstShot, secondShot;
                    int siteId = 0;
                    string name = string.Empty;
                    string zipCode = string.Empty;
                    string date = string.Empty;
                    if (typeValue == "xml")
                    {
                        // Parse XML file content
                        var rootElement = XElement.Parse(fileContent);
                        LambdaLogger.Log($"Root element: {rootElement}");

                        // Access the XML elements and retrieve their values
                        var siteElement = rootElement.Element("site");
                        if (siteElement != null && int.TryParse(siteElement.Attribute("id")?.Value, out int parsedId))
                        {
                            siteId = parsedId;
                            LambdaLogger.Log($"Site ID: {siteId}");
                        }

                        name = siteElement?.Element("name")?.Value;
                        zipCode = siteElement?.Element("zipCode")?.Value;


                        XmlDocument doc = new XmlDocument();
                        doc.LoadXml(fileContent);

                        // Get the data element
                        XmlElement data = doc.DocumentElement;

                        // Get the month, day and year attributes
                        string month = data.GetAttribute("month");
                        string day = data.GetAttribute("day");
                        string year = data.GetAttribute("year");

                        DateTime date1 = new DateTime(int.Parse(year), int.Parse(month), int.Parse(day));

                        date = $"{day}-{month}-{year}";

                        LambdaLogger.Log($"Date to string: {date}");
                        total = 0;
                        firstShot = 0;
                        secondShot = 0;

                        // Iterate through the vaccine elements and retrieve the values
                        foreach (var vaccineElement in rootElement.Element("vaccines").Elements("brand"))
                        {
                            var totalElement = vaccineElement.Element("total");
                            var firstShotElement = vaccineElement.Element("firstShot");
                            var secondShotElement = vaccineElement.Element("secondtShot");


                            if (totalElement != null && int.TryParse(totalElement.Value, out int parsedTotal) &&
                                firstShotElement != null && int.TryParse(firstShotElement.Value, out int parsedFirstShot) &&
                                secondShotElement != null && int.TryParse(secondShotElement.Value, out int parsedSecondShot))
                            {
                                total += parsedTotal;
                                firstShot += parsedFirstShot;
                                secondShot += parsedSecondShot;
                            }
                        }

                    }


                    else if (typeValue == "json")
                    {
                        // Parse JSON file content
                        var jsonObject = JsonDocument.Parse(fileContent);
                        LambdaLogger.Log($"jsonObject: {jsonObject}");

                        // Access the properties using the TryGetProperty method
                        JsonElement siteElement;
                        if (jsonObject.RootElement.TryGetProperty("site", out siteElement) && siteElement.ValueKind == JsonValueKind.Object)
                        {
                            JsonElement idElement;
                            if (siteElement.TryGetProperty("id", out idElement))
                            {
                                // Convert the idElement to an integer
                                siteId = idElement.ValueKind == JsonValueKind.Number ? idElement.GetInt32() : Convert.ToInt32(idElement.GetString());
                            }

                            JsonElement nameElement;
                            if (siteElement.TryGetProperty("name", out nameElement) && nameElement.ValueKind == JsonValueKind.String)
                            {
                                name = nameElement.GetString();
                            }

                            JsonElement zipCodeElement;
                            if (siteElement.TryGetProperty("zipCode", out zipCodeElement) && zipCodeElement.ValueKind == JsonValueKind.String)
                            {
                                zipCode = zipCodeElement.GetString();
                            }
                        }

                        // Retrieve the date properties and construct the date string
                        var dateElement = jsonObject.RootElement.GetProperty("date");
                        var day = 0;
                        var month = 0;
                        var year = 0;

                        if (dateElement.TryGetProperty("day", out var dayElement) && dayElement.ValueKind == JsonValueKind.Number)
                        {
                            day = dayElement.GetInt32();
                        }

                        if (dateElement.TryGetProperty("month", out var monthElement) && monthElement.ValueKind == JsonValueKind.Number)
                        {
                            month = monthElement.GetInt32();
                        }

                        if (dateElement.TryGetProperty("year", out var yearElement) && yearElement.ValueKind == JsonValueKind.Number)
                        {
                            year = yearElement.GetInt32();
                        }

                        date = $"{day}-{month}-{year}";

                        total = 0;
                        firstShot = 0;
                        secondShot = 0;

                        // Iterate through the vaccines array and retrieve the values
                        var vaccinesElement = jsonObject.RootElement.GetProperty("vaccines");
                        if (vaccinesElement.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var vaccineElement in vaccinesElement.EnumerateArray())
                            {
                                if (vaccineElement.TryGetProperty("total", out var totalElement) && totalElement.ValueKind == JsonValueKind.Number)
                                {
                                    total += totalElement.GetInt32();
                                }

                                if (vaccineElement.TryGetProperty("firstShot", out var firstShotElement) && firstShotElement.ValueKind == JsonValueKind.Number)
                                {
                                    firstShot += firstShotElement.GetInt32();
                                }

                                if (vaccineElement.TryGetProperty("secondShot", out var secondShotElement) && secondShotElement.ValueKind == JsonValueKind.Number)
                                {
                                    secondShot += secondShotElement.GetInt32();
                                }
                            }
                        }
                    }



                    else
                    {
                        // Invalid type value
                        throw new Exception($"Invalid type value: {typeValue}");
                    }

                    // Enter the data into the PostgreSQL database
                    using (var connection = new NpgsqlConnection(_connectionString))
                    {
                        connection.Open();

                        // Check if the site exists in the Sites table
                        using (var command = new NpgsqlCommand($"SELECT * FROM Sites WHERE SiteID=@siteId", connection))
                        {
                            command.Parameters.AddWithValue("@siteId", NpgsqlDbType.Integer, siteId);
                            using (var reader = command.ExecuteReader())
                            {
                                if (!reader.HasRows)
                                {
                                    // Insert a new row into the Sites table
                                    reader.Close();
                                    using (var insertCommand = new NpgsqlCommand("INSERT INTO Sites (SiteID, Name, ZipCode) VALUES (@siteId, @name, @zipCode)", connection))
                                    {
                                        insertCommand.Parameters.AddWithValue("@siteId", NpgsqlDbType.Integer, siteId);
                                        insertCommand.Parameters.AddWithValue("@name", NpgsqlDbType.Text, name);
                                        LambdaLogger.Log($"At the insert, sites table: {name}");
                                        insertCommand.Parameters.AddWithValue("@zipCode", NpgsqlDbType.Text, zipCode);
                                        try
                                        {
                                            insertCommand.ExecuteNonQuery();
                                            LambdaLogger.Log($"Inserted a new row into Sites table with SiteID={siteId}, Name='{name}', ZipCode='{zipCode}'");
                                        }
                                        catch (NpgsqlException e)
                                        {
                                            LambdaLogger.Log($"Failed to insert data into Sites table: {e.Message}");
                                        }
                                    }

                                }
                            }
                        }
                        LambdaLogger.Log($"date: {date}");


                        // Check if the data exists in the Data table
                        using (var command = new NpgsqlCommand($"SELECT * FROM Data WHERE SiteID=@siteId AND Date=@date", connection))
                        {
                            command.Parameters.AddWithValue("@siteId", NpgsqlDbType.Integer, siteId);
                            command.Parameters.AddWithValue("@date", NpgsqlDbType.Date, DateTime.ParseExact(date, "d-M-yyyy", CultureInfo.InvariantCulture)); // Assuming date format is "d-M-yyyy"

                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    // Update the existing row in the Data table
                                    reader.Close();
                                    using (var updateCommand = new NpgsqlCommand("UPDATE Data SET FirstShot=@firstShot, SecondShot=@secondShot WHERE SiteID=@siteId AND Date=@date", connection))
                                    {
                                        updateCommand.Parameters.AddWithValue("@firstShot", NpgsqlDbType.Integer, firstShot);
                                        updateCommand.Parameters.AddWithValue("@secondShot", NpgsqlDbType.Integer, secondShot);
                                        updateCommand.Parameters.AddWithValue("@siteId", NpgsqlDbType.Integer, siteId);
                                        updateCommand.Parameters.AddWithValue("@date", NpgsqlDbType.Date, DateTime.ParseExact(date, "d-M-yyyy", CultureInfo.InvariantCulture));

                                        try
                                        {
                                            updateCommand.ExecuteNonQuery();
                                            LambdaLogger.Log($"Updated an existing row in Data table with SiteID={siteId}, Date='{date}', FirstShot={firstShot}, SecondShot={secondShot}");
                                        }
                                        catch (NpgsqlException e)
                                        {
                                            LambdaLogger.Log($"Failed to update data in Data table: {e.Message}");
                                        }
                                    }
                                }
                                else
                                {
                                    // Insert a new row into the Data table
                                    reader.Close();
                                    using (var insertCommand = new NpgsqlCommand("INSERT INTO Data (SiteID, Date, FirstShot, SecondShot) VALUES (@siteId, @date, @firstShot, @secondShot)", connection))
                                    {
                                        insertCommand.Parameters.AddWithValue("@siteId", NpgsqlDbType.Integer, siteId);
                                        insertCommand.Parameters.AddWithValue("@date", NpgsqlDbType.Date, DateTime.ParseExact(date, "d-M-yyyy", CultureInfo.InvariantCulture));

                                        insertCommand.Parameters.AddWithValue("@firstShot", NpgsqlDbType.Integer, firstShot);
                                        insertCommand.Parameters.AddWithValue("@secondShot", NpgsqlDbType.Integer, secondShot);
                                        try
                                        {
                                            insertCommand.ExecuteNonQuery();
                                            LambdaLogger.Log($"Inserted a new row into Data table with SiteID={siteId}, Date='{date}', FirstShot={firstShot}, SecondShot={secondShot}");
                                        }
                                        catch (NpgsqlException e)
                                        {
                                            LambdaLogger.Log($"Failed to insert data into Data table: {e.Message}");
                                        }
                                    }
                                }
                            }
                        }

                    }
                }
            }
            catch (Exception e)
            {
                LambdaLogger.Log(e.Message);
                LambdaLogger.Log(e.StackTrace);
            }
        }
    }
}
