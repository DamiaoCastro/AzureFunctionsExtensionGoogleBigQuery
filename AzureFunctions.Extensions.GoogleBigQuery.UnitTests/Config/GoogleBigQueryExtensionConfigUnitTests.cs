using AzureFunctions.Extensions.GoogleBigQuery.Config;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.Config;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests.Config {

    [TestClass]
    public class GoogleBigQueryExtensionConfigUnitTests {

        private IExtensionConfigProvider objectToTest = new GoogleBigQueryExtensionConfig();

        [TestMethod]
        public void Initialize_Success() {

            //arrange
            var configuration = new Mock<IConfiguration>();
            var nameResolver = new Mock<INameResolver>();
            var converterManager = new Mock<IConverterManager>();
            var webHookProvider = new Mock<IWebHookProvider>();
            var extensionRegistry = new Mock<IExtensionRegistry>();

            var context = new ExtensionConfigContext(configuration.Object, nameResolver.Object, converterManager.Object, webHookProvider.Object, extensionRegistry.Object);

            //act 
            objectToTest.Initialize(context);

            //assert

        }

    }
}
