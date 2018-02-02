using Microsoft.Azure.WebJobs.Description;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    [Binding]
    [AttributeUsage(AttributeTargets.Parameter)]
    public class GoogleBigQueryManagementAttribute : GoogleBigQueryAttribute {

        public GoogleBigQueryManagementAttribute(string configurationNodeName) : base(configurationNodeName) { }

        public GoogleBigQueryManagementAttribute(string credentialsFileName, string projectId, string datasetId, string tableId) : base(credentialsFileName, projectId, datasetId, tableId) { }

        internal GoogleBigQueryManagementAttribute(string projectId, string datasetId, string tableId) : base(projectId, datasetId, tableId) { }

        internal GoogleBigQueryManagementAttribute(byte[] credentials, string projectId, string datasetId, string tableId) : base(credentials, projectId, datasetId, tableId) { }

    }
}
