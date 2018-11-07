using Microsoft.Azure.WebJobs.Description;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    [Binding]
    [AttributeUsage(AttributeTargets.Parameter)]
    public sealed class GoogleBigQueryAttribute : Attribute {

        /// <summary>
        /// Allows to write rows in Google BigQuery via the streaming API
        /// </summary>
        /// <param name="credentialsSettingKey">setting key that contains the credentials json.</param>
        /// <param name="projectId">project id ( differs from project name )</param>
        /// <param name="datasetId">dataset id</param>
        /// <param name="tableId">table id</param>
        public GoogleBigQueryAttribute(string credentialsSettingKey, string projectId, string datasetId, string tableId) {
            //if (string.IsNullOrWhiteSpace(credentialsSettingKey)) { throw new ArgumentNullException(nameof(credentialsSettingKey)); }
            //if (string.IsNullOrWhiteSpace(projectId)) { throw new ArgumentNullException(nameof(projectId)); }
            //if (string.IsNullOrWhiteSpace(datasetId)) { throw new ArgumentNullException(nameof(datasetId)); }
            //if (string.IsNullOrWhiteSpace(tableId)) { throw new ArgumentNullException(nameof(tableId)); }

            ////var credentialsString = Environment.GetEnvironmentVariable(credentialsSettingKey, EnvironmentVariableTarget.Process);
            //////if (string.IsNullOrWhiteSpace(credentialsString)) { throw new ArgumentNullException(nameof(credentialsString), $"The setting key '{credentialsSettingKey}' does not contain a value."); }
            ////if (!string.IsNullOrWhiteSpace(credentialsString)) { Credentials = System.Text.Encoding.UTF8.GetBytes(credentialsString); }
            //ProjectId = projectId;
            //DatasetId = datasetId;
            //TableId = tableId;
        }

        /// <summary>
        /// using this contructor, the settings will come from the configuration file.
        /// you should configure:
        /// 'configuration node name'.Credentials -> string representation of the JSON credential files given in the google cloud "service account" bit
        /// 'configuration node name'.ProjectId -> projectId where the refered google pubsub is contained in
        /// 'configuration node name'.DatasetId -> datasetId of the refered google bigquery
        /// 'configuration node name'.TableId -> tableId of the refered google bigquery
        /// </summary>
        /// <param name="settingsNodeName">prefix name that you gave to your configuration.</param>
        public GoogleBigQueryAttribute(string settingsNodeName) {
            if (string.IsNullOrWhiteSpace(settingsNodeName)) { throw new ArgumentNullException(nameof(settingsNodeName)); }
            ConfigurationNodeName = settingsNodeName;

            var credentialsSettingKey = $"{settingsNodeName}.{nameof(Credentials)}";
            var projectId = Environment.GetEnvironmentVariable($"{settingsNodeName}.{nameof(ProjectId)}", EnvironmentVariableTarget.Process);
            var datasetId = Environment.GetEnvironmentVariable($"{settingsNodeName}.{nameof(DatasetId)}", EnvironmentVariableTarget.Process);
            var tableId = Environment.GetEnvironmentVariable($"{settingsNodeName}.{nameof(TableId)}", EnvironmentVariableTarget.Process);

            //BaseConstructor(credentialsSettingKey, projectId, datasetId, tableId);
        }

        //private void BaseConstructor(string credentialsSettingKey, string projectId, string datasetId, string tableId) {
        //    if (string.IsNullOrWhiteSpace(credentialsSettingKey)) { throw new ArgumentNullException(nameof(credentialsSettingKey)); }
        //    var credentialsString = Environment.GetEnvironmentVariable(credentialsSettingKey, EnvironmentVariableTarget.Process);
        //    //if (string.IsNullOrWhiteSpace(credentialsString)) { throw new ArgumentNullException(nameof(credentialsString), $"The setting key '{credentialsSettingKey}' does not contain a value."); }
        //    //if (string.IsNullOrWhiteSpace(projectId)) { throw new ArgumentNullException(nameof(projectId)); }
        //    //if (string.IsNullOrWhiteSpace(datasetId)) { throw new ArgumentNullException(nameof(datasetId)); }
        //    //if (string.IsNullOrWhiteSpace(tableId)) { throw new ArgumentNullException(nameof(tableId)); }

        //    Credentials = System.Text.Encoding.UTF8.GetBytes(credentialsString);
        //    ProjectId = projectId;
        //    DatasetId = datasetId;
        //    TableId = tableId;
        //}

        internal byte[] Credentials { get; }

        public string ConfigurationNodeName { get; }

        public string ProjectId { get; }

        public string DatasetId { get; }

        public string TableId { get; }

        //public override int GetHashCode() {
        //    return $"{ConfigurationNodeName}|{ProjectId}|{DatasetId}|{TableId}".GetHashCode();
        //}

    }
}