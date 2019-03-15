using System;
using System.IO;
using System.Linq;
using System.Reflection.Metadata;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobStorageTransfer
{
    public static class CrossAccountBlobTransfer
    {
        private static string[] PermittedCoolBlobTierStorageKinds = { "BlobStorage", "BlockBlobStorage", "StorageV2" };

        [FunctionName("CrossAccountBlobTransfer")]
        public static async Task RunAsync(
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
                var sasSourceBlobUrl = GetShareAccessUri(inputBlob);

                await archiveBlob.StartCopyAsync(
                    new Uri(sasSourceBlobUrl))
                    .ConfigureAwait(false);

                log.LogInformation("Blob copy started");

                var copying = true;
                while (copying)
                {
                    // add in some delay here 
                    await Task.Delay(500);
                    await archiveBlob.FetchAttributesAsync().ConfigureAwait(false);
                    copying = archiveBlob.CopyState.Status == CopyStatus.Pending;
                }

                var accountProperties = await container.GetAccountPropertiesAsync().ConfigureAwait(false);

                if (!PermittedCoolBlobTierStorageKinds.Contains(accountProperties.AccountKind))
                {
                    log.LogWarning($"Unable to set target blob access tier to 'Cool' as the target storage account " +
                        $"(${accountProperties.AccountKind}) does not support blob access tier settings");
                }
                else
                {
                    await archiveBlob.SetStandardBlobTierAsync(StandardBlobTier.Cool).ConfigureAwait(false);
                    log.LogInformation("Access tier for target blob set to 'cool'");
                }

                log.LogInformation($"Archived {name} to {container.Uri} backup storage");
            }
            catch (StorageException se)
            {
                log.LogError($"Failed to copy blob to storage due to storage exception", se, se.Message);
            }
            catch (Exception e)
            {
                log.LogError($"Failed to copy blob to storage due to unexpected exception", e, e.Message);
            }
        }

        private static string GetShareAccessUri(CloudBlob sourceBlob)
        {
            int validMins = 300;
            var policy = new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessStartTime = null,
                SharedAccessExpiryTime = DateTimeOffset.Now.AddMinutes(validMins)
            };

            var sas = sourceBlob.GetSharedAccessSignature(policy);
            return sourceBlob.Uri.AbsoluteUri + sas;
        }
    }
}
