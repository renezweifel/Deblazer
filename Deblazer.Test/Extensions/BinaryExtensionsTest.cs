using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Dg;
using Dg.Deblazer.Extensions;

namespace Dg.Test.Extensions
{
    [TestClass]
    public class BinaryExtensionsTest
    {
        [TestMethod]
        public void BinaryExtension_FromUlongAndBack_GivesOriginalValue()
        {
            ulong testValue = 0x123456789abcdef;
            Assert.AreEqual(testValue, testValue.ToBinary().RowVersionToUInt64());
        }
    }
}
