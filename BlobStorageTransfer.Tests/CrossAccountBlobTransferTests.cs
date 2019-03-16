using Microsoft.WindowsAzure.Storage.Blob;
using Moq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using BlobStorageTransfer.Copying;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using Xunit;
using Xunit.Sdk;

namespace BlobStorageTransfer.Tests
{
    public class CrossAccountBlobTransferTests
    {
        private static readonly string TargetContainerName = "TargetArchive";
        private static readonly string SourceBlobName = "SourceBlob";
        private readonly IBlobCopyService blobCopyService;
        private readonly CloudBlockBlob sourceBlob;
        private readonly CloudBlockBlob targetBlob;
        private readonly CloudBlobContainer targetContainer;
        private readonly ILogger logger;

        public CrossAccountBlobTransferTests()
        {
            sourceBlob = CreateCloudBlockBlob();
            targetBlob = CreateCloudBlockBlob();
            targetContainer = CreateCloudBlobContainer();
            blobCopyService = Mock.Of<IBlobCopyService>();
            logger = Mock.Of<ILogger>();
            
            Mock.Get(targetContainer)
                .Setup(x => x.GetBlockBlobReference(TargetContainerName))
                .Returns(targetBlob);

            Mock.Get(targetBlob)
                .Setup(x => x.ExistsAsync())
                .Returns(Task.FromResult(true));

            Mock.Get(sourceBlob)
                .Setup(x => x.Name)
                .Returns(SourceBlobName);

            Mock.Get(targetBlob)
                .Setup(x => x.Name)
                .Returns(SourceBlobName);

            Mock.Get(blobCopyService)
                .Setup(x => x.CopyAsync(targetBlob, It.IsAny<Uri>()))
                .Returns(Task.FromResult(CopyStatus.Success));

            Mock.Get(blobCopyService)
                .Setup(x => x.SetAccessTierAsync(targetBlob, StandardBlobTier.Cool))
                .Returns(Task.FromResult(true));
        }

        [Fact]
        public async Task GivenABlobTriggerIsExecuted_WhenRunIsInvoked_ThenACloudBlockBlobIsExpected()
        {
            var target = new CrossAccountBlobTransfer(blobCopyService);
            await target.RunAsync(sourceBlob, targetContainer, TargetContainerName, logger);

            Mock.Get(targetContainer).Verify(x => x.CreateIfNotExistsAsync(), Times.Once);
        }

        [Fact]
        public async Task GivenACloudBlobTriggerIsExecuted_WhenRunIsInvoked_ASaSUrlIsRequestedFromTheSourceBlobForReadAccessOnly()
        {
            var target = new CrossAccountBlobTransfer(blobCopyService);
            await target.RunAsync(sourceBlob, targetContainer, TargetContainerName, logger);

            Mock.Get(blobCopyService).Verify(x =>
                x.CopyAsync(targetBlob, It.Is<Uri>(uri => uri.AbsoluteUri.EndsWith("&sp=r"))));
        }

        [Fact]
        public async Task GivenACloudBlobTriggerIsExecuted_WhenRunIsInvoked_ASaSUrlIsRequestedFromTheSourceBlobFor5MinutesOnly()
        {
            var target = new CrossAccountBlobTransfer(blobCopyService);
            await target.RunAsync(sourceBlob, targetContainer, TargetContainerName, logger);

            var currentTimePlus5Minutes = DateTime.UtcNow.AddMinutes(5);
            var seQueryParam = $"&se={currentTimePlus5Minutes:yyyy-MM-dd}T{currentTimePlus5Minutes:HH}%3A{currentTimePlus5Minutes:mm}%3A{currentTimePlus5Minutes:ss}Z";

            Mock.Get(blobCopyService).Verify(x =>
                x.CopyAsync(targetBlob, It.Is<Uri>(uri => uri.AbsoluteUri.Contains(seQueryParam))));
        }

        [Fact]
        public async Task GivenACloudBlobTriggerIsExecuted_WhenRunIsInvoked_TheArchiveBlobAccessTierIsSetToCool()
        {
            var target = new CrossAccountBlobTransfer(blobCopyService);
            await target.RunAsync(sourceBlob, targetContainer, TargetContainerName, logger);

            Mock.Get(blobCopyService).Verify(x => x.SetAccessTierAsync(targetBlob, StandardBlobTier.Cool));
        }

        [Fact]
        public async Task GivenACloudBlobTriggerIsEAxecuted_WhenCopyingFails_AStorageExceptionIsThrown()
        {
            Mock.Get(blobCopyService)
                .Setup(x => x.CopyAsync(targetBlob, It.IsAny<Uri>()))
                .Returns(Task.FromResult(CopyStatus.Aborted));

            var expectedMessage = $"Failed to complete copy of {sourceBlob.Uri.AbsoluteUri} to {targetBlob.Uri.AbsoluteUri}. Copy state was {CopyStatus.Aborted}";
            var target = new CrossAccountBlobTransfer(blobCopyService);
            var exception = await Assert.ThrowsAsync<StorageException>(async () => 
                await target.RunAsync(sourceBlob, targetContainer, TargetContainerName, logger));

            Assert.Equal(expectedMessage, exception.Message);
        }

        private CloudBlobContainer CreateCloudBlobContainer()
        {
            var container = new Mock<CloudBlobContainer>(MockBehavior.Loose,
                new Uri("http://mock.to/container"),
                new StorageCredentials("fakeaccount", Convert.ToBase64String(Encoding.Unicode.GetBytes("fakekeyval")), "fakekeyname"));

            return container.Object;
        }

        private CloudBlockBlob CreateCloudBlockBlob()
        {
            var blob = new Mock<CloudBlockBlob>(MockBehavior.Loose,
                new Uri("http://mock.to/container/file.ext"),
                new StorageCredentials("fakeaccount", Convert.ToBase64String(Encoding.Unicode.GetBytes("fakekeyval")), "fakekeyname"));

            return blob.Object;
        }
    }
}
