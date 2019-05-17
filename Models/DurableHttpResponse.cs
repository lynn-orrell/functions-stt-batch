using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;

namespace Lo.BatchTranscription
{
    public class DurableHttpResponse
    {
        public DurableHttpResponse(HttpStatusCode statusCode, string reasonPhrase, IDictionary<string, string[]> headers, string content)
        {
            StatusCode = statusCode;
            ReasonPhrase = reasonPhrase;
            Headers = headers;
            Content = content;
        }

        public static async Task<DurableHttpResponse> CreateAsync(HttpResponseMessage httpResponseMessage)
        {
            var headers = new Dictionary<string, string[]>();
            foreach(var header in httpResponseMessage.Headers)
            {
                headers.Add(header.Key, new StringValues(header.Value.ToArray()));
            }

            var content = await httpResponseMessage.Content.ReadAsStringAsync();

            return new DurableHttpResponse(httpResponseMessage.StatusCode, httpResponseMessage.ReasonPhrase, headers, content);
        }

        public HttpStatusCode StatusCode { get; }
        public string ReasonPhrase { get; }
        public IDictionary<string, string[]> Headers { get; }
        public string Content { get; }
    }
}