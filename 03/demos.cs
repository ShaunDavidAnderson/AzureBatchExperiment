using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace OptionPricerEngine
{
    class Program
    {
        [DllImport("simulation_engine.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern float CalculateCallValueCpu(OptionData optionData, int paths);

        const int NumberOfPaths = 262000;

        static void Main(string[] args)
        {
            Console.WriteLine("Option Pricer starting up");

            var inputFile = args[0];
            Console.WriteLine($"Input file is {inputFile}");

            var outputContainerSas = args[1];
            Console.WriteLine($"OutputContainerSas is {outputContainerSas}");

            var jobId = args[2];
            Console.WriteLine($"JobId is {jobId}");

            var allOptionData = ReadOptionDataFromFile(inputFile);

            Console.WriteLine("Calculating results...");
            var results = allOptionData.Select(x => new SimulationResult { OptionId = x.Id, CallValue = CalculateCallValueCpu(x, NumberOfPaths) });
            var csvResults = CreateCsvTextFromResults(results);

            Console.WriteLine("Uploading results to blob storage...");
            UploadContentToContainerAsync(jobId, csvResults, outputContainerSas).Wait();

            Console.WriteLine("Option Pricer finished!");
        }

        private static string CreateCsvTextFromResults(IEnumerable<SimulationResult> results)
        {
            using (var stringWriter = new StringWriter())
            {
                var csv = new CsvWriter(stringWriter);
                csv.WriteRecords(results);
                return stringWriter.ToString();
            }
        }

        private static IList<OptionData> ReadOptionDataFromFile(string optionDataFile)
        {
            using (var reader = new StreamReader(optionDataFile))
            {
                var csv = new CsvReader(reader);
                csv.Configuration.RegisterClassMap(new OptionDataMap());
                var allOptionData = csv.GetRecords<OptionData>().ToList();
                return allOptionData;
            }
        }

        private static async System.Threading.Tasks.Task UploadContentToContainerAsync(string jobId, string content, string containerSas)
        {
            try
            {
                CloudBlobContainer container = new CloudBlobContainer(new Uri(containerSas));
                CloudBlockBlob blob = container.GetBlockBlobReference(jobId);
                await blob.UploadTextAsync(content);

                Console.WriteLine("Successfully uploaded text for SAS URL " + containerSas);
            }
            catch (StorageException e)
            {
                Console.WriteLine("Failed to upload text for SAS URL " + containerSas);
                Console.WriteLine("Additional error information: " + e.Message);

                // Environment.ExitCode = -1;
            }
        }
    }
    
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    public struct OptionData
    {
        public int Id;
        public float S;
        public float X;
        public float T;
        public float R;
        public float V;
    }

    public class SimulationResult
    {
        public int OptionId { get; set; }
        public float CallValue { get; set; }
    }

    public class OptionDataMap : ClassMap<OptionData>
    {
        public OptionDataMap()
        {
            Map(x => x.Id);
            Map(x => x.S);
            Map(x => x.X);
            Map(x => x.T);
            Map(x => x.R);
            Map(x => x.V);
        }
    }


}
