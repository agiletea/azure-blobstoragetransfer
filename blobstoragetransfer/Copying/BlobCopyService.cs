using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobStorageTransfer.Copying
{
    [ExcludeFromCodeCoverage]
    internal sealed class BlobCopyService : IBlobCopyService
    {
        private static readonly string[] PermittedCoolBlobTierStorageKinds = { "BlobStorage", "BlockBlobStorage", "StorageV2" };
        private readonly ILogger log;

        public BlobCopyService(ILoggerFactory loggerFactory)
        {
            this.log = loggerFactory.CreateLogger<BlobCopyService>();
        }

        public async Task<CopyStatus> CopyAsync(CloudBlob targetBlob, Uri sourceUri)
        {
            await targetBlob.StartCopyAsync(sourceUri).ConfigureAwait(false);

            log.LogInformation("Blob copy started");

            var copying = true;
            while (copying)
            {
                // add in some delay here 
                await Task.Delay(500);
                await targetBlob.FetchAttributesAsync().ConfigureAwait(false);
                copying = targetBlob.CopyState.Status == CopyStatus.Pending;
            }

            return targetBlob.CopyState.Status;
        }

        public async Task<bool> SetAccessTierAsync(CloudBlockBlob targetBlob, StandardBlobTier tier)
        {
            var accountProperties = await targetBlob.Container.GetAccountPropertiesAsync().ConfigureAwait(false);

            if (!PermittedCoolBlobTierStorageKinds.Contains(accountProperties.AccountKind))
            {
                log.LogWarning($"Unable to set target blob access tier to 'Cool' as the target storage account " +
                               $"(${accountProperties.AccountKind}) does not support blob access tier settings");
                return false;
            }

            await targetBlob.SetStandardBlobTierAsync(tier).ConfigureAwait(false);
            log.LogInformation("Access tier for target blob set to 'cool'");
            return true;
            
        }
    }
}