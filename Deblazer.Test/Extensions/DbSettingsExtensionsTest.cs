using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dg.Deblazer.Settings;

namespace dg.Deblazer.Test.Extensions
{
    [TestClass]
    public class DbSettingsExtensionsTest
    {
        [TestMethod]
        public void DbSettingsExtensions_DeviniteDatabaseNameIsAccessed_EscapedDatabaseNameIsReturned()
        {
            string dbName = DbSettingsExtensions.GetDatabaseName(connectionString: "Data Source=SRV-SQL03.intranet.digitec;Initial Catalog=dev-devinite; Integrated Security=True");
            Assert.AreEqual("[dev-devinite]", dbName);
        }
        [TestMethod]
        public void DbSettingsExtensions_DeviniteDatabaseUnescapedNameIsAccessed_UnescapedDatabaseNameIsReturned()
        {
            string dbName = DbSettingsExtensions.GetDatabaseNameUnescaped(connectionString: "Data Source=SRV-SQL03.intranet.digitec;Initial Catalog=dev-devinite; Integrated Security=True");
            Assert.AreEqual("dev-devinite", dbName);
        }
    }
}
