using AzureFunctions.Extensions.GoogleBigQuery.TestsCommon;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AzureFunctions.Extensions.GoogleBigQuery.UnitTests {
    [TestClass]
    public class TableSchemaBuilderServiceTests {

        [TestMethod]
        public void GetTableSchema_TestBigQueryRow() {

            //Act
            var (tabelSchema, properties) = TableSchemaBuilderService.GetTableSchema(typeof(TestBigQueryRow));

            //Assert
            Assert.IsNotNull(tabelSchema);
            Assert.AreEqual(69, tabelSchema.Fields.Count);

            Assert.AreEqual("my description for this column", tabelSchema.Fields[0].Description);
            Assert.AreEqual("Bool", tabelSchema.Fields[0].Name);
            Assert.AreEqual("REQUIRED", tabelSchema.Fields[0].Mode);
            Assert.AreEqual("BOOLEAN", tabelSchema.Fields[0].Type);
            Assert.IsNull(tabelSchema.Fields[0].Fields);

            //...

            Assert.IsNull(tabelSchema.Fields[4].Description);
            Assert.AreEqual("BoolEnumerable", tabelSchema.Fields[4].Name);
            Assert.AreEqual("REPEATED", tabelSchema.Fields[4].Mode);
            Assert.AreEqual("BOOLEAN", tabelSchema.Fields[4].Type);
            Assert.IsNull(tabelSchema.Fields[4].Fields);
            
            //...

            Assert.IsNull(tabelSchema.Fields[65].Description);
            Assert.AreEqual("Record1", tabelSchema.Fields[65].Name);
            Assert.AreEqual("NULLABLE", tabelSchema.Fields[65].Mode);
            Assert.AreEqual("RECORD", tabelSchema.Fields[65].Type);
            Assert.IsNotNull(tabelSchema.Fields[65].Fields);

            var subFields65 = tabelSchema.Fields[65].Fields;
            Assert.AreEqual(2, subFields65.Count);
            Assert.IsNull(subFields65[0].Description);
            Assert.AreEqual("MySubProperty1", subFields65[0].Name);
            Assert.AreEqual("NULLABLE", subFields65[0].Mode);
            Assert.AreEqual("INTEGER", subFields65[0].Type);
            Assert.IsNull(subFields65[0].Fields);
            Assert.AreEqual("MySubProperty2", subFields65[1].Name);
            Assert.AreEqual("NULLABLE", subFields65[1].Mode);
            Assert.AreEqual("STRING", subFields65[1].Type);
            Assert.IsNull(subFields65[1].Fields);

            Assert.IsNull(tabelSchema.Fields[66].Description);
            Assert.AreEqual("Record2", tabelSchema.Fields[66].Name);
            Assert.AreEqual("REPEATED", tabelSchema.Fields[66].Mode);
            Assert.AreEqual("RECORD", tabelSchema.Fields[66].Type);
            Assert.IsNotNull(tabelSchema.Fields[66].Fields);

            var subFields66 = tabelSchema.Fields[66].Fields;
            Assert.AreEqual(2, subFields66.Count);

            Assert.IsNull(tabelSchema.Fields[67].Description);
            Assert.AreEqual("Record3", tabelSchema.Fields[67].Name);
            Assert.AreEqual("REPEATED", tabelSchema.Fields[67].Mode);
            Assert.AreEqual("RECORD", tabelSchema.Fields[67].Type);
            Assert.IsNotNull(tabelSchema.Fields[67].Fields);

            var subFields67 = tabelSchema.Fields[67].Fields;
            Assert.AreEqual(2, subFields67.Count);

            Assert.IsNull(tabelSchema.Fields[68].Description);
            Assert.AreEqual("incongruency", tabelSchema.Fields[68].Name);
            Assert.AreEqual("NULLABLE", tabelSchema.Fields[68].Mode);
            Assert.AreEqual("INTEGER", tabelSchema.Fields[68].Type);
            Assert.IsNull(tabelSchema.Fields[68].Fields);

        }

    }
}
