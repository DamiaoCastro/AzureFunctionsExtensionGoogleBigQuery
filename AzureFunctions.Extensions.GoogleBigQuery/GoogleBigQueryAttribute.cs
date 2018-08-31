using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Description;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    [Binding]
    [AttributeUsage(AttributeTargets.ReturnValue | AttributeTargets.Parameter)]
    public sealed class GoogleBigQueryAttribute : Attribute {

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

        internal GoogleBigQueryAttribute(byte[] credentials, string projectId, string datasetId, string tableId) {
            if (string.IsNullOrWhiteSpace(projectId)) { throw new ArgumentNullException(nameof(projectId)); }
            if (string.IsNullOrWhiteSpace(datasetId)) { throw new ArgumentNullException(nameof(datasetId)); }
            if (string.IsNullOrWhiteSpace(tableId)) { throw new ArgumentNullException(nameof(tableId)); }

            Credentials = credentials ?? throw new ArgumentNullException(nameof(credentials));
            ProjectId = projectId;
            DatasetId = datasetId;
            TableId = tableId;
        }

        internal GoogleBigQueryAttribute(string projectId, string datasetId, string tableId) {
            if (string.IsNullOrWhiteSpace(projectId)) { throw new ArgumentNullException(nameof(projectId)); }
            if (string.IsNullOrWhiteSpace(datasetId)) { throw new ArgumentNullException(nameof(datasetId)); }
            if (string.IsNullOrWhiteSpace(tableId)) { throw new ArgumentNullException(nameof(tableId)); }

            ProjectId = projectId;
            DatasetId = datasetId;
            TableId = tableId;
        }

        /// <summary>
        /// using this contructor, the settings will come from the configuration file.
        /// you should configure:
        /// 'your configuration node name'.Credentials -> string representation of the JSON credential files given in the google cloud "service account" bit
        /// 'your configuration node name'.ProjectId -> projectId where the refered google pubsub is contained in
        /// 'your configuration node name'.DatasetId -> datasetId of the refered google bigquery
        /// 'your configuration node name'.TableId -> tableId of the refered google bigquery
        /// </summary>
        /// <param name="configurationNodeName">prefix name that you gave to your configuration.</param>
        public GoogleBigQueryAttribute(string configurationNodeName) {
            if (string.IsNullOrWhiteSpace(configurationNodeName)) { throw new ArgumentNullException(nameof(configurationNodeName)); }

            ConfigurationNodeName = configurationNodeName;
        }

        public string CredentialsFileName { get; }
        internal byte[] Credentials { get; }

        public string ProjectId { get; }
        public string DatasetId { get; }
        public string TableId { get; }

        public string ConfigurationNodeName { get; }

        internal static GoogleBigQueryAttribute GetAttributeByConfiguration(GoogleBigQueryAttribute googleBigQueryAttribute) {
            if (string.IsNullOrWhiteSpace(googleBigQueryAttribute.ConfigurationNodeName)) { return googleBigQueryAttribute; }

            var credentialsString = Environment.GetEnvironmentVariable($"{googleBigQueryAttribute.ConfigurationNodeName}.{nameof(Credentials)}", EnvironmentVariableTarget.Process);
            var credentialsFileName = Environment.GetEnvironmentVariable($"{googleBigQueryAttribute.ConfigurationNodeName}.{nameof(CredentialsFileName)}", EnvironmentVariableTarget.Process);
            var projectId = Environment.GetEnvironmentVariable($"{googleBigQueryAttribute.ConfigurationNodeName}.{nameof(ProjectId)}", EnvironmentVariableTarget.Process);
            var datasetId = Environment.GetEnvironmentVariable($"{googleBigQueryAttribute.ConfigurationNodeName}.{nameof(DatasetId)}", EnvironmentVariableTarget.Process);
            var tableId = Environment.GetEnvironmentVariable($"{googleBigQueryAttribute.ConfigurationNodeName}.{nameof(TableId)}", EnvironmentVariableTarget.Process);

            GoogleBigQueryAttribute newGoogleBigQueryAttribute = null;
            if (string.IsNullOrWhiteSpace(credentialsFileName) && string.IsNullOrWhiteSpace(credentialsString)) {
                newGoogleBigQueryAttribute = new GoogleBigQueryAttribute(projectId, datasetId, tableId);
            } else {
                if (string.IsNullOrWhiteSpace(credentialsString)) {                    
                    newGoogleBigQueryAttribute = new GoogleBigQueryAttribute(credentialsFileName, projectId, datasetId, tableId);
                } else {
                    var credentials = System.Text.Encoding.UTF8.GetBytes(credentialsString);
                    newGoogleBigQueryAttribute = new GoogleBigQueryAttribute(credentials, projectId, datasetId, tableId);
                }
            }

            return newGoogleBigQueryAttribute;
        }

    }
}