using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobStorageTransfer.Copying
{
    public interface IBlobCopyService
    {
        Task<CopyStatus> CopyAsync(CloudBlob targetBlob, Uri sourceUri);
        Task<bool> SetAccessTierAsync(CloudBlockBlob targetBlob, StandardBlobTier tier);
    }
}
