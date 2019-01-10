using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace OptionPricerBatchRunner
{
    class Program
    {
        private const string StorageAccountName = "globomantics";
        private const string StorageAccountKey = "<insert key here>";

        private const string BatchAccountName = "globomantics";
        private const string BatchAccountUrl = "https://globomantics.centralus.batch.azure.com";
        private const string BatchAccountKey = "<insert key here>";


        private const string AppContainerName = "application";
        private const string InputContainerName = "input";
        private const string OutputContainerName = "output";

        private const string ApplicationFile = "OptionPricerEngine.tar.gz";

        private const string PoolIdPrefix = "optionpricerpool";
        private const string JobIdPrefix = "optionpricerjob";

        static async Task Main(string[] args)
        {
            var poolId = PoolIdPrefix + DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var jobId = JobIdPrefix + DateTime.Now.ToString("yyyyMMdd_HHmmss");

            var blobClient = CreateBlobClient();
            await CreateStorageContainersIfNotExist(blobClient);

            var application = await UploadFileOrGetReference(blobClient, AppContainerName, ApplicationFile, false);

            var outputContainerSasUrl = GetOutputContainerSasUrl(blobClient);

            var batchCredentials = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);
            using (var batchClient = BatchClient.Open(batchCredentials))
            {
                await CreatePoolIfNotExist(batchClient, poolId, new[] { application });
                await CreateJob(batchClient, jobId, poolId);
                await AddAllTasksToJob(jobId, blobClient, outputContainerSasUrl, batchClient);
                var tasksSucceeded = await MonitorTasks(batchClient, jobId, TimeSpan.FromMinutes(60));
                if (tasksSucceeded)
                {
                    await DownloadFromContainer(blobClient, OutputContainerName, Directory.GetCurrentDirectory());
                }

                await batchClient.PoolOperations.DeletePoolAsync(poolId);
            }

        }

        private static async Task DownloadFromContainer(CloudBlobClient blobClient, string containerName, string saveIntoDirectory)
        {
            Console.WriteLine($"Downloading all files from container {containerName}");
            var container = blobClient.GetContainerReference(containerName);
            var items = await container.ListBlobsSegmentedAsync(null, true, new BlobListingDetails(), null, new BlobContinuationToken(),
                new BlobRequestOptions(), new Microsoft.WindowsAzure.Storage.OperationContext());

            await Task.WhenAll(items.Results.Select(item =>
            {
                var blob = (CloudBlob)item;
                var localOutputFile = Path.Combine(saveIntoDirectory, blob.Name);
                return blob.DownloadToFileAsync(localOutputFile, FileMode.Create);
            }).ToArray());

            Console.WriteLine($"All files downloaded to {saveIntoDirectory}");
        }

        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            var tasksSuccessful = true;

            var tasks = await batchClient.JobOperations.ListTasks(jobId, new ODATADetailLevel(selectClause: "id")).ToListAsync();

            Console.WriteLine($"Waiting for tasks to complete. Timeout is {timeout}");

            var taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();

            try
            {
                await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
            }
            catch
            {
                await batchClient.JobOperations.TerminateJobAsync(jobId, "Job timed out");
                Console.WriteLine("Job timed out");
                return false;
            }

            await batchClient.JobOperations.TerminateJobAsync(jobId, "All tasks completed");

            foreach (var task in tasks)
            {
                await task.RefreshAsync(new ODATADetailLevel(selectClause: "id, executionInfo"));
                if (task.ExecutionInformation.Result == TaskExecutionResult.Failure)
                {
                    tasksSuccessful = false;
                    Console.WriteLine($"Task {task.Id} encountered a failure: {task.ExecutionInformation.FailureInformation.Message}");
                }
            }

            if (tasksSuccessful)
            {
                Console.WriteLine("All tasks completed successfully!");
            }

            return tasksSuccessful;
            
        }

        private static async Task AddAllTasksToJob(string jobId, CloudBlobClient blobClient, string outputContainerSasUrl, BatchClient batchClient)
        {
            var inputFiles = Directory.EnumerateFiles(".", "Options*.csv");

            var tasks = await Task.WhenAll(inputFiles.Select(async inputFile =>
            {
                var uploadedFile = await UploadFileOrGetReference(blobClient, InputContainerName, inputFile, true);
                return CreateTask(jobId, uploadedFile, outputContainerSasUrl);
            }).ToArray());

            await batchClient.JobOperations.AddTaskAsync(jobId, tasks);
        }

        private static CloudTask CreateTask(string jobId, ResourceFile inputFile, string outputContainerSasUrl)
        {
            var taskId = "simulationTask_" + Path.GetFileNameWithoutExtension(inputFile.FilePath);
            var commandLine = $@"/bin/bash -c ""cd $AZ_BATCH_NODE_SHARED_DIR && ./OptionPricerEngine $AZ_BATCH_TASK_WORKING_DIR/{inputFile.FilePath} \""{outputContainerSasUrl}\"" {jobId}_{taskId}""";
            var task = new CloudTask(taskId, commandLine);
            task.ResourceFiles = new[] { inputFile };
            return task;
        }

        private static async Task CreateJob(BatchClient batchClient, string jobId, string poolId)
        {
            Console.WriteLine($"Creating job {jobId}");
            var job = batchClient.JobOperations.CreateJob();
            job.Id = jobId;
            job.PoolInformation = new PoolInformation { PoolId = poolId };

            await job.CommitAsync();
        }

        private static async Task CreatePoolIfNotExist(BatchClient batchClient, string poolId, ResourceFile[] resourceFiles)
        {
            var pools = await batchClient.PoolOperations.ListPools().ToListAsync();
            var alreadyExists = pools.Any(x => x.Id == poolId);
            if (alreadyExists)
            {
                Console.WriteLine($"Pool {poolId} already exists, no need to create");
                return;
            }

            Console.WriteLine($"Creating pool {poolId}");

            var pool = batchClient.PoolOperations.CreatePool(
                poolId: poolId,
                targetLowPriorityComputeNodes: 10,
                virtualMachineSize: "Standard_A1",
                virtualMachineConfiguration: new VirtualMachineConfiguration(
                    new ImageReference("UbuntuServer", "Canonical", "16.04-LTS"),
                    "batch.node.ubuntu 16.04"));

            pool.StartTask = new StartTask
            {
                CommandLine = $@"/bin/bash -c ""sudo apt-get update && sudo apt-get install -y libcurl3 && cd $AZ_BATCH_NODE_SHARED_DIR && tar -xvf $AZ_BATCH_NODE_STARTUP_DIR/wd/{ApplicationFile}""",
                UserIdentity = new UserIdentity(new AutoUserSpecification(AutoUserScope.Pool, ElevationLevel.Admin)),
                WaitForSuccess = true,
                ResourceFiles = resourceFiles
            };

            await pool.CommitAsync();
        }

        private static string GetOutputContainerSasUrl(CloudBlobClient blobClient)
        {
            var accessPolicy = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Write
            };

            var container = blobClient.GetContainerReference(OutputContainerName);
            var accessToken = container.GetSharedAccessSignature(accessPolicy);
            return $"{container.Uri}{accessToken}";
        }

        private static async Task<ResourceFile> UploadFileOrGetReference(CloudBlobClient blobClient, string containerName, string file, bool overwrite)
        {
            var blobName = Path.GetFileName(file);
            var container = blobClient.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            var alreadyExists = await blob.ExistsAsync();

            if (!alreadyExists || overwrite)
            {
                Console.WriteLine($"Uploading file {file} to container {containerName}");
                await blob.UploadFromFileAsync(file);
            }

            var accessPolicy = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            var accessToken = blob.GetSharedAccessSignature(accessPolicy);
            var blobUri = $"{blob.Uri}{accessToken}";

            return new ResourceFile(blobUri, blobName);
        }

        private static async Task CreateStorageContainersIfNotExist(CloudBlobClient blobClient)
        {
            await CreateContainerIfNotExist(blobClient, AppContainerName);
            await CreateContainerIfNotExist(blobClient, InputContainerName);
            await CreateContainerIfNotExist(blobClient, OutputContainerName);
        }

        private static async Task CreateContainerIfNotExist(CloudBlobClient blobClient, string containerName)
        {
            var container = blobClient.GetContainerReference(containerName);

            if (await container.CreateIfNotExistsAsync())
            {
                Console.WriteLine($"Container {containerName} created");
            }
            else
            {
                Console.WriteLine($"Container {containerName} already exists");
            }
        }

        private static CloudBlobClient CreateBlobClient()
        {
            var connectionString = $"DefaultEndpointsProtocol=https;AccountName={StorageAccountName};AccountKey={StorageAccountKey}";
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            return storageAccount.CreateCloudBlobClient();
        }
    }
}
