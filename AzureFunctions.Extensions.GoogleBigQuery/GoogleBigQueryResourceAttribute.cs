using Microsoft.Azure.WebJobs.Description;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery {

    [Binding]
    [AttributeUsage(AttributeTargets.Parameter)]
    public sealed class GoogleBigQueryResourceAttribute : GoogleBigQueryBaseAttribute {

        public GoogleBigQueryResourceAttribute(string credentialsSettingKey) : base(credentialsSettingKey) { }

        internal override string GetObjectKey() {
            return $"{CredentialsSettingKey}";
        }

    }
}