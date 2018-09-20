using AzureFunctions.Extensions.GoogleBigQuery.TestsCommon;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests {

    [TestClass]
    public class GoogleBigQueryRowSerializationUnitTests {

        public class GoogleBigQueryRowExtended : GoogleBigQueryRow {
            public GoogleBigQueryRowExtended(DateTime? date, string insertId) : base(date, insertId) {
            }

            [Newtonsoft.Json.JsonProperty]
            [System.ComponentModel.DataAnnotations.Schema.Column]
            public int test { get; set; }

        }

        [TestMethod]
        public void GoogleBigQueryRow_JObject_Serialize() {

            //Arrange
            var date = DateTime.UtcNow.Date;
            var insertId = Guid.NewGuid().ToString();
            var jObject = JsonConvert.DeserializeObject<JObject>("{ \"test\": 1 }");
            var row = new GoogleBigQueryRowJObject(date, insertId, jObject);

            //Act
            var serialization = JsonConvert.SerializeObject(row);

            //Assert
            Assert.IsTrue(serialization.Contains("test"));
            Assert.AreEqual(date, ((IGoogleBigQueryRow)row).getPartitionDate());
            Assert.AreEqual(insertId, ((IGoogleBigQueryRow)row).getInsertId());

        }

        [TestMethod]
        public void GoogleBigQueryRow_Extend_Serialize() {

            //Arrange
            var date = DateTime.UtcNow.Date;
            var insertId = Guid.NewGuid().ToString();
            var row = new GoogleBigQueryRowExtended(date, insertId);
            row.test = 1;

            //Act
            var serialization = JsonConvert.SerializeObject(row);

            //Assert
            Assert.AreEqual("{\"test\":1}", serialization);
            Assert.AreEqual(date, ((IGoogleBigQueryRow)row).getPartitionDate());
            Assert.AreEqual(insertId, ((IGoogleBigQueryRow)row).getInsertId());

        }

    }
}
