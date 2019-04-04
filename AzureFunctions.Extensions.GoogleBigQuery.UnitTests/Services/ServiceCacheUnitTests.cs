using AzureFunctions.Extensions.GoogleBigQuery.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests.Services {
    [TestClass]
    public class ServiceCacheUnitTests {

        private IServiceCache objToTest = new ServiceCache();

        [TestMethod]
        public void GetFromCache_NoValue_Success() {

            //arrange
            GoogleBigQueryBaseAttribute googleBigQueryAttribute = new GoogleBigQueryCollectorAttribute("settingsNodeName");

            //act
            var value = objToTest.GetFromCache<IBigQueryService>(googleBigQueryAttribute);

            //assert
            Assert.IsNull(value);

        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetFromCache_NullAttribute_Error() {

            //arrange

            //act
            var value = objToTest.GetFromCache<IBigQueryService>(null);

            //assert

        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void SaveToCache_NullAttribute_Error() {

            //arrange

            //act
            objToTest.SaveToCache<dynamic>(null, new { }, 10);

            //assert

        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void SaveToCache_NullValue_Error() {

            //arrange
            GoogleBigQueryBaseAttribute googleBigQueryAttribute = new GoogleBigQueryCollectorAttribute("settingsNodeName");

            //act
            objToTest.SaveToCache<IBigQueryService>(googleBigQueryAttribute, null, 10);

            //assert

        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void SaveToCache_NegativeMinutesToCache_Error() {

            //arrange
            GoogleBigQueryBaseAttribute googleBigQueryAttribute = new GoogleBigQueryCollectorAttribute("settingsNodeName");

            //act
            objToTest.SaveToCache<dynamic>(googleBigQueryAttribute, new { }, -1);

            //assert

        }

        [TestMethod]
        public void SaveToCache_ValueRepleacement_Success() {

            //arrange
            GoogleBigQueryBaseAttribute googleBigQueryAttribute = new GoogleBigQueryCollectorAttribute("settingsNodeName");

            //act
            objToTest.SaveToCache<dynamic>(googleBigQueryAttribute, new { test = 1 }, 10);
            objToTest.SaveToCache<dynamic>(googleBigQueryAttribute, new { test = 2 }, 10);
            var value = objToTest.GetFromCache<dynamic>(googleBigQueryAttribute);

            //assert
            Assert.IsNotNull(value);
            Assert.AreEqual(2, value.test);

        }

    }
}
