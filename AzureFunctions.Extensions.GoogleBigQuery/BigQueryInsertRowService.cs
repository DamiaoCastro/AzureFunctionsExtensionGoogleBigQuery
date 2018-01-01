using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace AzureFunctions.Extensions.GoogleBigQuery {
    public class BigQueryInsertRowService {

        private const string BigQueryDateTimeFormat = "yyyy-MM-dd HH:mm:ss";
        private static System.Globalization.CultureInfo cultureUS = new System.Globalization.CultureInfo("en-US");

        public static BigQueryInsertRow GetBigQueryInsertRow(GoogleBigQueryRow row, IDictionary<string, IEnumerable<PropertyInfo>> dictionaryOfProperties) {
            if (row == null) { throw new System.ArgumentNullException(nameof(row)); }
            if (dictionaryOfProperties == null) { throw new System.ArgumentNullException(nameof(dictionaryOfProperties)); }

            IDictionary<string, object> dic = GetDictionaryOfValues(dictionaryOfProperties, row);

            return new BigQueryInsertRow(row.InsertId) { dic };
        }

        private static IDictionary<string, object> GetDictionaryOfValues(IDictionary<string, IEnumerable<PropertyInfo>> dictionaryOfProperties, object obj) {
            if (obj == null) { return null; }

            var properties = dictionaryOfProperties[obj.GetType().FullName];
            IDictionary<string, object> dictionaryOfValues = properties.ToDictionary(c => c.Name, c => GetBigQueryValue(dictionaryOfProperties, c, obj));

            return dictionaryOfValues;
        }

        private static object GetBigQueryValue(IDictionary<string, IEnumerable<PropertyInfo>> dictionaryOfProperties, PropertyInfo property, object obj) {
            switch (property.PropertyType.Name.ToUpper()) {
                case "BYTE":
                    return (int)(byte)property.GetValue(obj);
                case "CHAR":
                    return ((char)property.GetValue(obj)).ToString();
                case "CHAR[]":
                    return ((char[])property.GetValue(obj)).ToString();
                case "DATETIME":
                    var datetimeValue = (DateTime)property.GetValue(obj);
                    return datetimeValue.ToString(BigQueryDateTimeFormat, cultureUS);
                case "DECIMAL":
                    return (float)(decimal)property.GetValue(obj);
                case "GUID":
                    return ((Guid)property.GetValue(obj)).ToString();
                case "UINT64":
                    return (int)(UInt64)property.GetValue(obj);
                case "IENUMERABLE`1":
                    var enumerableValue = property.GetValue(obj);

                    Type innerPropertyType = property.PropertyType.GenericTypeArguments[0];
                    switch (innerPropertyType.Name.ToUpper()) {
                        case "BYTE":
                            return enumerableValue;
                        case "BOOLEAN":
                            return ((bool[])enumerableValue);
                        case "CHAR":
                            return ((char[])enumerableValue).ToString();
                        case "DATETIME":
                            return (DateTime[])enumerableValue;
                        case "DATETIMEOFFSET":
                            return (DateTimeOffset[])enumerableValue;
                        case "DOUBLE":
                            return ((double[])enumerableValue).Select(c => (float)c).ToArray();
                        case "DECIMAL":
                            return ((decimal[])enumerableValue).Select(c => (float)c).ToArray();
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
                            if (property.PropertyType.IsArray) {
                                return enumerableValue;
                            }

                            IEnumerable<object> ie = (IEnumerable<object>)enumerableValue;
                            if (innerPropertyType.IsClass && innerPropertyType.Namespace != "System") {
                                return (IEnumerable<Dictionary<string, object>>) ie.Select(c => GetDictionaryOfValues(dictionaryOfProperties, c)).ToArray();
                            } else {
                                var length = ie.Count();
                                var i = Array.CreateInstance(innerPropertyType, length);
                                Array.Copy(ie.ToArray(), i, length);
                                return i;
                            }
                    }

                default:
                    if (property.PropertyType.IsClass && property.PropertyType.Namespace != "System") {//crappy but works for now
                        if (property.PropertyType.IsArray) {
                            var array = (IEnumerable<object>)property.GetValue(obj);
                            //return array.Select(c => GetDictionaryOfValues(dictionaryOfProperties, c)).ToArray();
                            return null;
                        } else {
                            return GetDictionaryOfValues(dictionaryOfProperties, property.GetValue(obj));
                        }
                    }

                    return property.GetValue(obj);
            }
        }

    }
}