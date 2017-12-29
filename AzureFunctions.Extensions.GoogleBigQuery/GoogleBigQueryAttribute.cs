using Microsoft.Azure.WebJobs.Description;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    [Binding]
    [AttributeUsage(AttributeTargets.ReturnValue | AttributeTargets.Parameter)]
    public class GoogleBigQueryAttribute : Attribute {

        /// <summary>
        /// Allows to write rows in Google BigQuery 
        /// </summary>
        /// <param name="credentialsFileName">json file name that is expected to be in the bin folder. 
        /// Don't forget to set the property of that file to "copy always" to the output directory.
        /// ex: "mycredentials.json"</param>
        /// <param name="projectId">project id ( differs from project name )</param>
        /// <param name="datasetId">dataset id</param>
        /// <param name="tableId">table id</param>
        public GoogleBigQueryAttribute(string credentialsFileName, string projectId, string datasetId, string tableId) {
            if (string.IsNullOrWhiteSpace(credentialsFileName)) { throw new ArgumentNullException(nameof(credentialsFileName)); }
            if (string.IsNullOrWhiteSpace(projectId)) { throw new ArgumentNullException(nameof(projectId)); }
            if (string.IsNullOrWhiteSpace(datasetId)) { throw new ArgumentNullException(nameof(datasetId)); }
            if (string.IsNullOrWhiteSpace(tableId)) { throw new ArgumentNullException(nameof(tableId)); }

            CredentialsFileName = credentialsFileName;
            ProjectId = projectId;
            DatasetId = datasetId;
            TableId = tableId;
        }

        public string CredentialsFileName { get; }
        public string ProjectId { get; }
        public string DatasetId { get; }
        public string TableId { get; }

    }
}