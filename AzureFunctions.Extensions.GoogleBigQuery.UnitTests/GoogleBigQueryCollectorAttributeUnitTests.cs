using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests {

    [TestClass]
    public class GoogleBigQueryCollectorAttributeUnitTests {

        [TestMethod]
        public void Constructor_SettingsNodeName_Success() {

            //arrange
            Environment.SetEnvironmentVariable("settingsNodeName.DatasetId", "testDatasetId");
            Environment.SetEnvironmentVariable("settingsNodeName.ProjectId", "testProjectId");
            Environment.SetEnvironmentVariable("settingsNodeName.TableId", "testTableId");

            //act
            var attribute = new GoogleBigQueryCollectorAttribute("settingsNodeName");

            //assert
            Assert.AreEqual("settingsNodeName", attribute.ConfigurationNodeName);
            Assert.AreEqual("settingsNodeName.Credentials", attribute.CredentialsSettingKey);
            Assert.AreEqual("testDatasetId", attribute.DatasetId);
            Assert.AreEqual("testProjectId", attribute.ProjectId);
            Assert.AreEqual("testTableId", attribute.TableId);

        }

        [TestMethod]
        public void Constructor_SettingCredentialKey_Success() {

            //arrange
            
            //act
            var attribute = new GoogleBigQueryCollectorAttribute("credentialsSettingKey", "testProjectId1", "testDatasetId2", "testTableId3");

            //assert
            Assert.IsNull(attribute.ConfigurationNodeName);
            Assert.AreEqual("credentialsSettingKey", attribute.CredentialsSettingKey);
            Assert.AreEqual("testProjectId1", attribute.ProjectId);
            Assert.AreEqual("testDatasetId2", attribute.DatasetId);
            Assert.AreEqual("testTableId3", attribute.TableId);

        }

        [TestMethod]
        public void Constructor_SettingCredentialAndTableFullName_Success() {

            //arrange
            Environment.SetEnvironmentVariable("tableFullNameSettingKey", "testProjectId.testDatasetId.testTableId");

            //act
            var attribute = new GoogleBigQueryCollectorAttribute("credentialsSettingKey", "tableFullNameSettingKey");

            //assert
            Assert.IsNull(attribute.ConfigurationNodeName);
            Assert.AreEqual("credentialsSettingKey", attribute.CredentialsSettingKey);
            Assert.AreEqual("testProjectId", attribute.ProjectId);
            Assert.AreEqual("testDatasetId", attribute.DatasetId);
            Assert.AreEqual("testTableId", attribute.TableId);

        }

        [TestMethod]
        public void Constructor_SettingCredentialAndTableFullName_BadSettingValue() {

            //arrange
            Environment.SetEnvironmentVariable("tableFullNameSettingKey", "xxxx");

            //act
            var attribute = new GoogleBigQueryCollectorAttribute("credentialsSettingKey", "tableFullNameSettingKey");

            //assert
            Assert.IsNull(attribute.ConfigurationNodeName);
            Assert.AreEqual("credentialsSettingKey", attribute.CredentialsSettingKey);
            Assert.IsNull(attribute.ProjectId);
            Assert.IsNull(attribute.DatasetId);
            Assert.IsNull(attribute.TableId);

        }

        [TestMethod]
        public void Constructor_SettingCredentialAndTableFullName_OldTableFullNameFormat() {

            //arrange
            Environment.SetEnvironmentVariable("tableFullNameSettingKey", "projectId:datasetId.tableId");

            //act
            var attribute = new GoogleBigQueryCollectorAttribute("credentialsSettingKey", "tableFullNameSettingKey");

            //assert
            Assert.IsNull(attribute.ConfigurationNodeName);
            Assert.AreEqual("credentialsSettingKey", attribute.CredentialsSettingKey);
            Assert.AreEqual("projectId", attribute.ProjectId);
            Assert.AreEqual("datasetId", attribute.DatasetId);
            Assert.AreEqual("tableId", attribute.TableId);

        }

    }
}
