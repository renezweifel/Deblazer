using Dg.Deblazer.Visitors;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using Dg.Deblazer.ContextValues;
using Dg.Deblazer.Write;
using System;

namespace Dg.Deblazer.Test.Write
{
    public class WriteDefaultsTestEntity : DbEntity, IInsertDate
    {
        public int CustomInt { get; set; }

        public string CustomString { get; set; }
        public DateTime InsertDate { get; set; }

        protected override void CheckProperties(IUpdateVisitor visitor)
        { }

        protected override void HandleChildren(DbEntityVisitorBase visitor)
        { }

        protected override void ModifyInternalState(FillVisitor visitor)
        { }
    }

    [TestClass]
    public class DefaultValuesTests
    {
        [TestMethod]
        public void WritingDefaultValues_InsertUserId_IsSet()
        {
            var writingDefaults = new WriteDefaultInsertValues(); ;

            var testEntities = new List<WriteDefaultsTestEntity>()
            {
                new WriteDefaultsTestEntity()
            };

            writingDefaults.SetDefaultValues(testEntities);
            Assert.AreNotEqual(default(DateTime), testEntities[0].InsertDate);
        }

        [TestMethod]
        public void WritingDefaultValues_CustomValue_IsSetWithAction()
        {
            var writingDefaults = new WriteDefaultInsertValues(); ;

            var testEntities = new List<WriteDefaultsTestEntity>()
            {
                new WriteDefaultsTestEntity()
            };

            writingDefaults.AddCustomValueSetter<WriteDefaultsTestEntity>(e => e.CustomInt = 1);

            writingDefaults.SetDefaultValues(testEntities);
            Assert.AreEqual(1, testEntities[0].CustomInt);
        }

        [TestMethod]
        public void WritingDefaultValues_MultipleCustomSetter_ValuesAreSet()
        {
            var writingDefaults = new WriteDefaultInsertValues(); ;

            var testEntities = new List<WriteDefaultsTestEntity>()
            {
                new WriteDefaultsTestEntity()
            };

            writingDefaults.AddCustomValueSetter<WriteDefaultsTestEntity>(e => e.CustomInt = 1);
            writingDefaults.AddCustomValueSetter<WriteDefaultsTestEntity>(e => e.CustomString = "Hello");

            writingDefaults.SetDefaultValues(testEntities);

            Assert.AreEqual(1, testEntities[0].CustomInt);
            Assert.AreEqual("Hello", testEntities[0].CustomString);
        }
    }
}
