// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Storage;
using System;
using Microsoft.Azure.Storage.Blob;
using System.IO;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Net;
using Microsoft.Extensions.Primitives;
using System.Linq;

namespace Lo.BatchTranscription
{
    public class TranscribeAudioFunctions
    {
        private readonly IHttpClientFactory _httpClientFactory;

        public TranscribeAudioFunctions(IHttpClientFactory httpClientFactory)
        {
            _httpClientFactory = httpClientFactory;
        }

        [FunctionName(nameof(TranscribeAudio))]
        public async Task TranscribeAudio([EventGridTrigger]EventGridEvent eventGridEvent, [OrchestrationClient]DurableOrchestrationClient starter, ILogger log)
        {
            await starter.StartNewAsync(nameof(DurableTranscribeAudio), eventGridEvent);
        }

        [FunctionName(nameof(DurableTranscribeAudio))]
        public async Task DurableTranscribeAudio([OrchestrationTrigger] DurableOrchestrationContext context, ILogger log)
        {
            var eventGridEvent = context.GetInput<EventGridEvent>();

            string transcriptionStatusUrl = null;
            DateTime expiryTime = context.CurrentUtcDateTime.AddHours(24);
            while(context.CurrentUtcDateTime < expiryTime)
            {
                var response = await context.CallActivityAsync<DurableHttpResponse>(nameof(PostAudioForTranscription), eventGridEvent);

                int statusCode = (int)response.StatusCode;
                if(statusCode >= 200 && statusCode <= 299)
                {
                    transcriptionStatusUrl = new StringValues(response.Headers["Location"]);
                    log.LogInformation($"Transcription Status Url: {transcriptionStatusUrl}");
                    break;
                }
                else if(response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    TimeSpan retrySpan = response.Headers.ContainsKey("RetryAfter") && 
                        !string.IsNullOrEmpty(new StringValues(response.Headers["RetryAfter"])) ? TimeSpan.FromSeconds(int.Parse(new StringValues(response.Headers["RetryAfter"]))) : TimeSpan.FromMinutes(1);
                    
                    var nextCheckpoint = context.CurrentUtcDateTime.Add(retrySpan);
                    
                    if(!context.IsReplaying)
                    {
                        log.LogInformation($"***** Rate limit on batch transcription hit. Retrying again at: {nextCheckpoint}");
                    }

                    await context.CreateTimer(nextCheckpoint, CancellationToken.None);
                }
                else
                {
                    throw new Exception($"Received an non-successful HTTP status code while submitting audio for transcription. Status Code: {response.StatusCode}. Reason: {response.ReasonPhrase}");
                }
            }

            await context.CallSubOrchestratorAsync(nameof(MonitorTranscription), transcriptionStatusUrl);
        }

        [FunctionName(nameof(PostAudioForTranscription))]
        public async Task<DurableHttpResponse> PostAudioForTranscription([ActivityTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            var audioBlobSasUrl = GetAudioBlobSasUrl(eventGridEvent);

            log.LogInformation($"Audio Url: {audioBlobSasUrl}");

            var request = new HttpRequestMessage(HttpMethod.Post, $"https://{Environment.GetEnvironmentVariable("SpeechRegion")}.cris.ai/api/speechtotext/v2.0/transcriptions");
            request.Headers.Add("Ocp-Apim-Subscription-Key", Environment.GetEnvironmentVariable("SpeechKey"));
            request.Content = new StringContent(JsonConvert.SerializeObject(new { 
                Name = eventGridEvent.Id,
                Description= eventGridEvent.Subject,
                Locale = "en-US",
                RecordingsUrl = audioBlobSasUrl,
                properties = new Dictionary<string, string>()
                {
                    { "PunctuationMode", "DictatedAndAutomatic" },
                    { "ProfanityFilterMode", "Masked" },
                    { "AddWordLevelTimestamps", "True" },
                    { "AddSentiment", "True"}
                }
            }), Encoding.UTF8, "application/json");

            var response = await _httpClientFactory.CreateClient().SendAsync(request);

            return await DurableHttpResponse.CreateAsync(response);
        }

        [FunctionName(nameof(MonitorTranscription))]
        public async Task MonitorTranscription([OrchestrationTrigger] DurableOrchestrationContext context, ILogger log)
        {
            var transcriptionStatusUrl = context.GetInput<string>();

            bool isTranscriptionComplete = false;
            DateTime expiryTime = context.CurrentUtcDateTime.AddHours(24);
            while(!isTranscriptionComplete && context.CurrentUtcDateTime < expiryTime)
            {
                if(!context.IsReplaying)
                {
                    log.LogInformation($"Monitoring Transcription at: {transcriptionStatusUrl}");
                }

                var jsonData = await context.CallActivityAsync<JObject>(nameof(GetTranscriptionStatus), transcriptionStatusUrl);
                var status = ((string)jsonData["status"]);
                var statusMessage = ((string)jsonData["statusMessage"]);

                switch(status.ToLower())
                {
                    case "running":
                    case "notstarted":
                        var nextCheckpoint = context.CurrentUtcDateTime.AddSeconds(60);
                        if(!context.IsReplaying)
                        {
                            log.LogInformation($"***** Transcription status: {status}. Checking status again at: {nextCheckpoint}");
                        }
                        await context.CreateTimer(nextCheckpoint, CancellationToken.None);
                        break;
                    case "succeeded":
                        isTranscriptionComplete = true;
                        await context.CallActivityAsync<Task>(nameof(SaveTranscription), jsonData);
                        break;
                    default:
                        isTranscriptionComplete = true;
                        log.LogWarning($"Transcription failed: {statusMessage}");
                        break;
                }
            }
        }

        [FunctionName(nameof(GetTranscriptionStatus))]
        public async Task<JObject> GetTranscriptionStatus([ActivityTrigger] string transcriptionStatusUrl, ILogger log)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, transcriptionStatusUrl);
            request.Headers.Add("Ocp-Apim-Subscription-Key", Environment.GetEnvironmentVariable("SpeechKey"));

            var response = await _httpClientFactory.CreateClient().SendAsync(request);
            
            var result = JObject.Parse(await response.Content.ReadAsStringAsync());
            log.LogDebug(result.ToString());

            return result;
        }

        [FunctionName(nameof(SaveTranscription))]
        public async Task SaveTranscription([ActivityTrigger] JObject transcriptionResult, ILogger log)
        {
            var resultsUrl = (string)transcriptionResult["resultsUrls"]["channel_0"];
            var originalAudioPath = (string)transcriptionResult["description"];
            log.LogInformation($"Result Url: {resultsUrl}");

            var request = new HttpRequestMessage(HttpMethod.Get, resultsUrl);
            request.Headers.Add("Ocp-Apim-Subscription-Key", Environment.GetEnvironmentVariable("SpeechKey"));

            var response = await _httpClientFactory.CreateClient().SendAsync(request);
            if(response.IsSuccessStatusCode)
            {
                log.LogInformation($"Saving transcription output...");
                var storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
                var blobClient = storageAccount.CreateCloudBlobClient();
                var audioContainer = blobClient.GetContainerReference("transcriptions");
                var transcriptionJsonBlob = audioContainer.GetBlockBlobReference($"{Path.GetFileNameWithoutExtension(originalAudioPath)}.json");
                
                Stream inputStream = await response.Content.ReadAsStreamAsync();
                using(Stream outputStream = await transcriptionJsonBlob.OpenWriteAsync())
                {
                    await inputStream.CopyToAsync(outputStream);
                }

                log.LogInformation($"Saved transcription output (json): {transcriptionJsonBlob.Uri}");

                var transcriptionTextBlob = audioContainer.GetBlockBlobReference($"{Path.GetFileNameWithoutExtension(originalAudioPath)}.txt");
                using(var textBlobStream = await transcriptionTextBlob.OpenWriteAsync())
                {
                    inputStream.Position = 0;
                    using(StreamReader streamReader = new StreamReader(inputStream))
                    using(JsonReader jsonReader = new JsonTextReader(streamReader))
                    {
                        byte[] bytes;
                        while(jsonReader.Read())
                        {
                            if(jsonReader.TokenType == JsonToken.PropertyName)
                            {
                                if(jsonReader.Value.Equals("Display"))
                                {
                                    jsonReader.Read();
                                    bytes = Encoding.UTF8.GetBytes($"{jsonReader.Value} ");
                                    await textBlobStream.WriteAsync(bytes, 0, bytes.Length);
                                }
                            }
                        }
                    }
                }

                log.LogInformation($"Saved transcription output (text): {transcriptionTextBlob.Uri}");
            }
        }

        private string GetAudioBlobSasUrl(EventGridEvent eventGridEvent)
        {
            var storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            var blobClient = storageAccount.CreateCloudBlobClient();
            var audioContainer = blobClient.GetContainerReference("audio");
            var audioBlob = audioContainer.GetBlockBlobReference(Path.GetFileName(eventGridEvent.Subject));

            var sharedAccessBlobPolicy = new SharedAccessBlobPolicy();
            sharedAccessBlobPolicy.SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddDays(1);
            sharedAccessBlobPolicy.Permissions = SharedAccessBlobPermissions.Read;

            var audioBlobSasToken = audioBlob.GetSharedAccessSignature(sharedAccessBlobPolicy);

            var jsonData = eventGridEvent.Data as JObject;
            var url = (string)jsonData["url"];

            return $"{url}{audioBlobSasToken}";
        }
    }
}
