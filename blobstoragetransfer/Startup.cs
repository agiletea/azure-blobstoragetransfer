using System.Diagnostics.CodeAnalysis;
using BlobStorageTransfer;
using BlobStorageTransfer.Copying;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

[assembly:WebJobsStartup(typeof(Startup))]
namespace BlobStorageTransfer
{
    [ExcludeFromCodeCoverage]
    public class Startup : IWebJobsStartup
    {
        public void Configure(IWebJobsBuilder builder)
        {
            builder.Services.AddLogging(logging =>
            {
                logging.AddFilter(level => true);
            });

            builder.Services.AddTransient<IBlobCopyService, BlobCopyService>();
        }
    }
}
