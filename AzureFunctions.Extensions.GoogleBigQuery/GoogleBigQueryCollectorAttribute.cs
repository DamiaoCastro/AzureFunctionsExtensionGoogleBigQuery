using Microsoft.Azure.WebJobs.Description;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    [Binding]
    [AttributeUsage(AttributeTargets.Parameter)]
    public sealed class GoogleBigQueryCollectorAttribute : GoogleBigQueryBaseAttribute {

        /// <summary>
        /// Use this attribute to bind to ICollector<IGoogleBigQueryRow>, meaning ICollector<*My*GoogleBigQueryRow> or ICollector<GoogleBigQueryRowJObject>.
        /// The values collected will be writen using the streaming API of BigQuery. Please note that the streaming API, when writting to DAY partioned tables has a data range limitation.
        /// More information about BigQuery streaming api in the page. https://cloud.google.com/bigquery/streaming-data-into-bigquery
        /// </summary>
        /// <param name="credentialsSettingKey">setting key where the value is the string representation of the JSON credential files given in the google cloud service account.</param>
        /// <param name="projectId">project id ( differs from project name )</param>
        /// <param name="datasetId">dataset id</param>
        /// <param name="tableId">table id</param>
        public GoogleBigQueryCollectorAttribute(string credentialsSettingKey, string projectId, string datasetId, string tableId) : base(credentialsSettingKey) {
            ProjectId = projectId;
            DatasetId = datasetId;
            TableId = tableId;
        }

        /// <summary>
        /// Use this attribute to bind to ICollector<IGoogleBigQueryRow>, meaning ICollector<*My*GoogleBigQueryRow> or ICollector<GoogleBigQueryRowJObject>.
        /// The values collected will be writen using the streaming API of BigQuery. Please note that the streaming API, when writting to DAY partioned tables has a data range limitation.
        /// More information about BigQuery streaming api in the page. https://cloud.google.com/bigquery/streaming-data-into-bigquery .
        /// ----
        /// using this contructor, the settings will come entirely from the settings of the application.
        /// you should configure:
        /// 'configuration node name'.Credentials -> setting key where the value is the string representation of the JSON credential files given in the google cloud service account.
        /// 'configuration node name'.ProjectId -> projectId where the refered google pubsub is contained in
        /// 'configuration node name'.DatasetId -> datasetId of the refered google bigquery
        /// 'configuration node name'.TableId -> tableId of the refered google bigquery
        /// </summary>
        /// <param name="settingsNodeName">prefix name that you gave to your settings.</param>
        public GoogleBigQueryCollectorAttribute(string settingsNodeName) : base($"{settingsNodeName}.{nameof(Credentials)}") {
            ConfigurationNodeName = settingsNodeName;

            if (!string.IsNullOrWhiteSpace(ConfigurationNodeName)) {
                var projectId = Environment.GetEnvironmentVariable($"{settingsNodeName}.{nameof(ProjectId)}", EnvironmentVariableTarget.Process);
                var datasetId = Environment.GetEnvironmentVariable($"{settingsNodeName}.{nameof(DatasetId)}", EnvironmentVariableTarget.Process);
                var tableId = Environment.GetEnvironmentVariable($"{settingsNodeName}.{nameof(TableId)}", EnvironmentVariableTarget.Process);

                ProjectId = projectId;
                DatasetId = datasetId;
                TableId = tableId;
            }
        }

        public string ConfigurationNodeName { get; }

        public string ProjectId { get; }

        public string DatasetId { get; }

        public string TableId { get; }

        internal override string GetObjectKey() {
            return $"{ConfigurationNodeName}|{CredentialsSettingKey}|{ProjectId}|{DatasetId}|{TableId}";
        }

    }
}