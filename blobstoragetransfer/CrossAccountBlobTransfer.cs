using System;
using System.IO;
using System.Linq;
using System.Reflection.Metadata;
using System.Threading.Tasks;
using BlobStorageTransfer.Copying;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobStorageTransfer
{
    public class CrossAccountBlobTransfer
    {
        private readonly IBlobCopyService blobCopyService;
        
        public CrossAccountBlobTransfer(IBlobCopyService blobCopyService)
        {
            this.blobCopyService = blobCopyService;
        }

        [FunctionName("CrossAccountBlobTransfer")]
        public async Task RunAsync(
            [BlobTrigger("%input-container%/{name}", Connection = "SourceContainer")]CloudBlockBlob inputBlob,
            [Blob("%output-container%/{name}", FileAccess.Write, Connection ="TargetContainer")]CloudBlobContainer container,
            string name, 
            ILogger log)
        {
            await inputBlob.FetchAttributesAsync().ConfigureAwait(false);

            log.LogInformation($"C# Blob trigger function Processing blob\n Name:{name} \n Size: {inputBlob.Properties.Length} Bytes");

            log.LogInformation($"Targeting container {container.Name}");

            await container.CreateIfNotExistsAsync().ConfigureAwait(false);

            var archiveBlob = container.GetBlockBlobReference(name);

            log.LogInformation($"Targeting blob {archiveBlob.Name}");

            var blobExists = await archiveBlob.ExistsAsync().ConfigureAwait(false);

            log.LogInformation($"Target blob exists? {blobExists}");

            try
            {
                var sasSourceBlobUrl = GetShareAccessUri(inputBlob, TimeSpan.FromMinutes(5));

                var copyResult = await blobCopyService.CopyAsync(archiveBlob, new Uri(sasSourceBlobUrl)).ConfigureAwait(false);

                if (copyResult != CopyStatus.Success)
                {
                    var error = $"Failed to complete copy of {inputBlob.Uri.AbsoluteUri} to {archiveBlob.Uri.AbsoluteUri}. Copy state was {copyResult}";
                    log.LogError(error);
                    throw new StorageException(error);
                }

                log.LogInformation($"Archived {name} to {container.Uri} backup storage");

                var setAccessTierResult = await blobCopyService.SetAccessTierAsync(archiveBlob, StandardBlobTier.Cool).ConfigureAwait(false);

                if (setAccessTierResult)
                {
                    log.LogInformation($"Set {name} access tier to cool");
                }
                else
                {
                    log.LogWarning($"Failed to set {name} access tier to cool");
                }
            }
            catch (StorageException se)
            {
                log.LogError($"Failed to copy blob to storage due to storage exception", se, se.Message);
                throw;
            }
            catch (Exception e)
            {
                log.LogError($"Failed to copy blob to storage due to unexpected exception", e, e.Message);
                throw;
            }
        }

        private static string GetShareAccessUri(CloudBlob sourceBlob, TimeSpan validityWindow)
        {
            var policy = new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessStartTime = null,
                SharedAccessExpiryTime = DateTimeOffset.Now.Add(validityWindow)
            };

            var sas = sourceBlob.GetSharedAccessSignature(policy);
            return sourceBlob.Uri.AbsoluteUri + sas;
        }
    }
}
