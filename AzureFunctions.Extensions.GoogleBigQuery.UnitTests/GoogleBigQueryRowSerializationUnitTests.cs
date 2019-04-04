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

            [JsonProperty]
            [System.ComponentModel.DataAnnotations.Schema.Column]
            public int Test { get; set; }

        }

        [TestMethod]
        public void GoogleBigQueryRowJObject_Serialize() {

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
            var row = new GoogleBigQueryRowExtended(date, insertId) {
                Test = 1
            };

            //Act
            var serialization = JsonConvert.SerializeObject(row);

            //Assert
            Assert.AreEqual("{\"Test\":1}", serialization);
            Assert.AreEqual(date, ((IGoogleBigQueryRow)row).getPartitionDate());
            Assert.AreEqual(insertId, ((IGoogleBigQueryRow)row).getInsertId());

        }

    }
}
