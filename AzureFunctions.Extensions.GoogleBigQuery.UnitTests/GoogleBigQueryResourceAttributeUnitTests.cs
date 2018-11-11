using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests {
    [TestClass]
    public class GoogleBigQueryResourceAttributeUnitTests {

        [TestMethod]
        public void Constructor_SettingsNodeName_Success() {

            //arrange
            
            //act
            var attribute = new GoogleBigQueryResourceAttribute("credentialsSettingKey");

            //assert
            Assert.AreEqual("credentialsSettingKey", attribute.CredentialsSettingKey);

        }

        [TestMethod]
        public void Constructor_NullParameter_Success() {

            //arrange

            //act
            var attribute = new GoogleBigQueryResourceAttribute(null);

            //assert
            Assert.IsNull(attribute.CredentialsSettingKey);

        }

    }
}