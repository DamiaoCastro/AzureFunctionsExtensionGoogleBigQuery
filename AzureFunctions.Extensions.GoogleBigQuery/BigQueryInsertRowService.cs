using Google.Cloud.BigQuery.V2;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace AzureFunctions.Extensions.GoogleBigQuery
{
    internal class BigQueryInsertRowService
    {

        private const string BigQueryDateTimeFormat = "yyyy-MM-dd HH:mm:ss";
        private static System.Globalization.CultureInfo cultureUS = new System.Globalization.CultureInfo("en-US");

        internal static BigQueryInsertRow GetBigQueryInsertRow(GoogleBigQueryRow row, IDictionary<string, IEnumerable<PropertyInfo>> dictionaryOfProperties)
        {
            if (row == null) { throw new System.ArgumentNullException(nameof(row)); }
            if (dictionaryOfProperties == null) { throw new System.ArgumentNullException(nameof(dictionaryOfProperties)); }

            IDictionary<string, object> dic = GetDictionaryOfValues(dictionaryOfProperties, row);

            if (string.IsNullOrWhiteSpace(row.InsertId))
            {
                return new BigQueryInsertRow() { dic };
            }
            else
            {
                return new BigQueryInsertRow(row.InsertId) { dic };
            }

        }

        private static IDictionary<string, object> GetDictionaryOfValues(IDictionary<string, IEnumerable<PropertyInfo>> dictionaryOfProperties, object obj)
        {
            if (obj == null) { return null; }

            var properties = dictionaryOfProperties[obj.GetType().FullName];
            IDictionary<string, object> dictionaryOfValues = properties.ToDictionary(c => c.Name, c => GetBigQueryValue(dictionaryOfProperties, c, obj));

            return dictionaryOfValues;
        }

        private static object GetBigQueryValue(IDictionary<string, IEnumerable<PropertyInfo>> dictionaryOfProperties, PropertyInfo property, object obj)
        {
            switch (property.PropertyType.Name.ToUpper())
            {
                case "IENUMERABLE`1":
                    return GetArrayFromEnumreable(dictionaryOfProperties, property, obj);
                case "NULLABLE`1":
                    var value = property.GetValue(obj);
                    if (value == null)
                    {
                        return null;
                    }
                    else
                    {
                        var propertyTypeName = property.PropertyType.GenericTypeArguments[0].Name;
                        return GetNonEnumerableBigQueryValue(propertyTypeName, value);
                    }
            }

            if (property.PropertyType.IsClass && property.PropertyType.Namespace != "System")
            {//crappy but works for now
                if (property.PropertyType.IsArray)
                {
                    var array = (IEnumerable<object>)property.GetValue(obj);
                    return GetSubEntitiesBigQueryInsertRows(dictionaryOfProperties, array);
                }
                else
                {
                    var value = property.GetValue(obj);
                    if (value == null)
                    {
                        return null;
                    }
                    else
                    {
                        return GetSubEntitiesBigQueryInsertRows(dictionaryOfProperties, new List<object> { value }).First();
                    }
                }
            }

            return GetNonEnumerableBigQueryValue(property.PropertyType.Name, property.GetValue(obj));
        }

        private static object GetNonEnumerableBigQueryValue(string propertyTypeName, object value)
        {
            switch (propertyTypeName.ToUpper())
            {
                case "BYTE":
                    return (int)(byte)value;
                case "CHAR":
                    return ((char)value).ToString();
                case "CHAR[]":
                    return ((char[])value).ToString();
                case "DATETIME":
                    return ((DateTime)value).ToString(BigQueryDateTimeFormat, cultureUS);
                case "DATETIMEOFFSET":
                    return ((DateTimeOffset)value).ToString(BigQueryDateTimeFormat, cultureUS);
                case "DECIMAL":
                    return (float)(decimal)value;
                case "GUID":
                    return ((Guid)value).ToString();
                case "UINT64":
                    return (int)(UInt64)value;
                default:
                    return value;
            }
        }

        private static object GetArrayFromEnumreable(IDictionary<string, IEnumerable<PropertyInfo>> dictionaryOfProperties, PropertyInfo property, object obj)
        {
            var enumerableValue = property.GetValue(obj);

            Type innerPropertyType = property.PropertyType.GenericTypeArguments[0];
            switch (innerPropertyType.Name.ToUpper())
            {
                case "BYTE":
                    return (byte[])enumerableValue;
                case "BOOLEAN":
                    return ((bool[])enumerableValue);
                case "CHAR":
                    return ((char[])enumerableValue).ToString();
                case "DATETIME":
                    return (DateTime[])enumerableValue;
                case "DATETIMEOFFSET":
                    return (DateTimeOffset[])enumerableValue;
                case "DOUBLE":
                    return ((IEnumerable<double>)enumerableValue).Select(c => (float)c).ToArray();
                case "DECIMAL":
                    return ((IEnumerable<decimal>)enumerableValue).Select(c => (float)c).ToArray();
                case "SINGLE":
                    return ((float[])enumerableValue);
                case "GUID":
                    return ((Guid[])enumerableValue).Select(c => c.ToString()).ToArray();
                case "UINT16":
                    return ((UInt16[])enumerableValue);
                case "INT16":
                    return ((Int16[])enumerableValue);
                case "INT":
                case "INT32":
                    return ((int[])enumerableValue);
                case "UINT32":
                    return ((UInt32[])enumerableValue);
                case "INT64":
                    return ((Int64[])enumerableValue);
                case "UINT64":
                    return ((UInt64[])enumerableValue);
                default:
                    if (property.PropertyType.IsArray)
                    {
                        return enumerableValue;
                    }

                    IEnumerable<object> ie = (IEnumerable<object>)enumerableValue;
                    if (innerPropertyType.IsClass && innerPropertyType.Namespace != "System")
                    {
                        return GetSubEntitiesBigQueryInsertRows(dictionaryOfProperties, ie);
                    }
                    else
                    {
                        var length = ie.Count();
                        var i = Array.CreateInstance(innerPropertyType, length);
                        Array.Copy(ie.ToArray(), i, length);
                        return i;
                    }
            }
        }

        internal static IEnumerable<string> GetBigQueryJobLines(IEnumerable<GoogleBigQueryRow> rows)
        {

            var jsonSerializerSettings = new JsonSerializerSettings()
            {
                DateFormatString = BigQueryDateTimeFormat,
                Culture = cultureUS
            };

            return rows.Select(r => JsonConvert.SerializeObject(r, jsonSerializerSettings));
        }

        private static BigQueryInsertRow[] GetSubEntitiesBigQueryInsertRows(IDictionary<string, IEnumerable<PropertyInfo>> dictionaryOfProperties, IEnumerable<object> objs)
        {

            if (objs.Count() > 0)
            {
                return objs.Select(c => new BigQueryInsertRow() { GetDictionaryOfValues(dictionaryOfProperties, c) }).ToArray();
            }

            return new BigQueryInsertRow[] { };
        }

    }
}