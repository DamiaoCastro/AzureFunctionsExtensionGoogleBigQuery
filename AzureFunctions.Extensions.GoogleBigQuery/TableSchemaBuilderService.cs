using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace AzureFunctions.Extensions.GoogleBigQuery {
    public class TableSchemaBuilderService {

        public static (TableSchema, IDictionary<string, IEnumerable<System.Reflection.PropertyInfo>>) GetTableSchema(Type tableType) {

            var properties = GetPropertyInfo(tableType);

            var dictionaryOfProperties = new Dictionary<string, IEnumerable<System.Reflection.PropertyInfo>>();
            dictionaryOfProperties.Add(tableType.FullName, properties);

            var fields = properties.Select(p => GetTableFieldSchema(p, dictionaryOfProperties));

            var schema = new Google.Apis.Bigquery.v2.Data.TableSchema() { Fields = fields.ToList() };

            return (schema, dictionaryOfProperties);
        }

        private static TableFieldSchema GetTableFieldSchema(PropertyInfo propertyInfo, Dictionary<string, IEnumerable<System.Reflection.PropertyInfo>> dictionaryOfProperties) {

            //var fields = from property in properties
            //             let mode = BigQueryFieldMode.Nullable
            //             //let mode = property.CustomAttributes.Any(a => a.AttributeType == typeof(RequiredAttribute)) ? BigQueryFieldMode.Required : BigQueryFieldMode.Nullable
            //             let type = GetBigQueryType(property.PropertyType)
            //             select new TableFieldSchema() { Name = property.Name, Type = type, Mode = mode.ToString() }
            //             ;

            var propertyType = propertyInfo.PropertyType;

            var descripton = propertyInfo
                                .CustomAttributes
                                .FirstOrDefault(a => a.AttributeType == typeof(DescriptionAttribute))
                                ?.ConstructorArguments[0].Value.ToString();

            string type = null;
            BigQueryFieldMode mode = BigQueryFieldMode.Nullable;
            IList<TableFieldSchema> fields = null;

            var innerPropertyType = propertyType;
            if (propertyType.Name.Equals("IEnumerable`1")) {
                innerPropertyType = propertyType.GenericTypeArguments[0];
            } else {
                if (propertyType.IsArray) {
                    innerPropertyType = propertyType.GetElementType();
                }
            }

            if (innerPropertyType.IsClass && innerPropertyType.Namespace != "System") {//crappy but works for now

                mode = BigQueryFieldMode.Nullable;
                if (propertyType.IsArray || propertyType.Name.Equals("IEnumerable`1")) {
                    mode = BigQueryFieldMode.Repeated;
                }

                IEnumerable<PropertyInfo> properties = null;
                if (dictionaryOfProperties.ContainsKey(innerPropertyType.FullName)) {
                    properties = dictionaryOfProperties[innerPropertyType.FullName];
                } else {
                    properties = GetPropertyInfo(innerPropertyType);
                    dictionaryOfProperties.Add(innerPropertyType.FullName, properties);
                }
                fields = properties.Select(p => GetTableFieldSchema(p, dictionaryOfProperties)).ToList();

                type = "RECORD";

            } else {
                (type, mode) = GetBigQueryTypeAndMode(propertyInfo);
            }

            return new TableFieldSchema() { Name = propertyInfo.Name, Type = type, Mode = mode.ToString().ToUpper(), Description = descripton, Fields = fields };

        }

        private static (string, BigQueryFieldMode) GetBigQueryTypeAndMode(PropertyInfo propertyInfo) {

            //in the GCP website, when creating a new table, it shows this options
            // differs from -> Google.Cloud.BigQuery.V2.BigQueryDbType

            //STRING
            //BYTES

            //INTEGER   
            //FLOAT
            //BOOLEAN

            //TIMESTAMP
            //DATE
            //TIME
            //DATETIME

            //RECORD

            //-----------------------------------

            string type = "STRING";
            BigQueryFieldMode fieldMode = GetBaseFiledMode(propertyInfo);

            Type propertyType = propertyInfo.PropertyType;

            if (propertyType.Name.Equals("Nullable`1")) {
                propertyType = propertyType.GenericTypeArguments[0];
                fieldMode = BigQueryFieldMode.Nullable;
            }

            bool isIEnumerable = false;
            if (propertyType.Name.Equals("IEnumerable`1")) {
                propertyType = propertyType.GenericTypeArguments[0];
                isIEnumerable = true;
            }

            var propertyTypeName = propertyType.Name.ToUpper();

            if (propertyType.IsArray) {
                propertyTypeName = propertyTypeName.TrimEnd('[', ']');
                isIEnumerable = true;
            }

            if (isIEnumerable && propertyTypeName != "BYTE") {
                fieldMode = BigQueryFieldMode.Repeated;
            }

            switch (propertyTypeName) {
                case "BOOLEAN":
                    type = "BOOLEAN";
                    break;
                case "CHAR":
                    fieldMode = GetBaseFiledMode(propertyInfo);
                    break;
                case "DECIMAL":
                case "DOUBLE":
                case "SINGLE":
                    type = "FLOAT";
                    break;
                case "DATETIME":
                case "DATETIMEOFFSET":
                    type = "DATETIME";
                    break;
                case "INT":
                case "INT16":
                case "INT32":
                case "INT64":
                case "UINT16":
                case "UINT32":
                case "UINT64":
                    type = "INTEGER";
                    break;
                case "BYTE":
                    if (isIEnumerable) {
                        type = "BYTES";
                        fieldMode = GetBaseFiledMode(propertyInfo);
                    } else {
                        type = "INTEGER";
                    }
                    break;
            }

            return (type, fieldMode);
        }

        private static BigQueryFieldMode GetBaseFiledMode(PropertyInfo propertyInfo) {
            return propertyInfo
                                                        .CustomAttributes
                                                        .Any(a => a.AttributeType == typeof(RequiredAttribute)) ? BigQueryFieldMode.Required : BigQueryFieldMode.Nullable;
        }

        private static IEnumerable<PropertyInfo> GetPropertyInfo(Type type) {
            return type
                    .GetProperties()
                    .Where(c => c.PropertyType.IsPublic && c.CustomAttributes.Any(a => a.AttributeType == typeof(ColumnAttribute)));
        }

    }
}