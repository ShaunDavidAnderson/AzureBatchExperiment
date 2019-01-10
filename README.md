# AzureBatchExperiment
Showing process to set up Azure Batch 

##Setting up storage in Blob containers (See Powershell template for automation)
- Input (Read)
- Application (Read)
- Output (Private)

###Uploads
- Application: Cant find tar.gz file

### Alternative example using R
- See this link: https://docs.microsoft.com/en-us/azure/batch/tutorial-r-doazureparallel
- Includes the doAzureParallel GitHub package: https://github.com/Azure/doAzureParallel
- this requires devtools::install_github("Azure/doAzureParallel") & devtools::install_github("Azure/rAzureBatch")

##Use Containers on Batch
- Build and configure an Azure COntainer Registry (Azures version of DockerHub)
- Buid and Deploy Container (Docker)
- Login to container Registry with CLI
- Tag and the push Docker file to registry (ACR)

(Todo: DO this with R)
