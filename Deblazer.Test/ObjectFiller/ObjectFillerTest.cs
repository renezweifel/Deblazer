using Dg;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using Dg.Deblazer.Read;

namespace Dg.Deblazer.Test
{
    [TestClass]
    public class ObjectFillerTest
    {
        [TestMethod]
        public void ObjectFiller_TargetTypeIsTypedId_ClassIsFilledCorrectly()
        {
            var dataRecord = new MockDataReader(
                new List<MockRecordField>()
                {
                    new MockRecordField(1, "productId", typeof(int))
                });

            var objectFiller = new ObjectFiller();

            var result = (Id<Product>)objectFiller.Build(typeof(Id<Product>), dataRecord);

            Assert.AreEqual(result.ToInt(), 1);
        }

        [TestMethod]
        public void ObjectFiller_TargetTypeIsNullableTypedId_ClassIsFilledCorrectly()
        {
            var dataRecord = new MockDataReader(
                new List<MockRecordField>()
                {
                    new MockRecordField(null,"productId", typeof(int))
                });

            var objectFiller = new ObjectFiller();

            var result = (Id<Product>?)objectFiller.Build(typeof(Id<Product>?), dataRecord);

            Assert.AreEqual(result, null);
        }

        [TestMethod]
        public void ObjectFiller_TargetTypeIsClassWithConstructor_ClassIsFilledCorrectly()
        {
            var today = Date.Today;
            var now = DateTime.Now;
            var dataRecord = new MockDataReader(
                new List<MockRecordField>()
                {
                    new MockRecordField(1           ,"productId"        , typeof(int)),
                    new MockRecordField(1L          ,"nullableProductId", typeof(long)),
                    new MockRecordField("woohoo!"   ,"someString"       , typeof(string)),
                    new MockRecordField(1.345356m   ,"price"            , typeof(decimal)),
                    new MockRecordField(null        ,"maybePrice"       , typeof(decimal)),
                    new MockRecordField(today       ,"date"             , "date", typeof(DateTime)),
                    new MockRecordField(now         ,"datetime"         , typeof(DateTime)),
                });

            var objectFiller = new ObjectFiller();

            var result = (ClassWithSingleConstructor)objectFiller.Build(typeof(ClassWithSingleConstructor), dataRecord);

            Assert.AreEqual(1, result.productId);
            Assert.AreEqual(new LongId<Product>(1) as LongId<Product>?, result.nullableProductId);
            Assert.AreEqual("woohoo!", result.someString);
            Assert.AreEqual(1.345356m, result.price);
            Assert.AreEqual(null, result.maybePrice);
            Assert.AreEqual(today, result.date);
            Assert.AreEqual(now, result.datetime);
        }

        [TestMethod]
        public void ObjectFiller_TargetTypeIsStruct_StructIsFilledCorrectly()
        {
            var today = Date.Today;
            var now = DateTime.Now;
            var dataRecord = new MockDataReader(
                new List<MockRecordField>()
                {
                    new MockRecordField(1           ,"productId"        , typeof(int)),
                    new MockRecordField(1L          ,"nullableProductId", typeof(long)),
                    new MockRecordField("woohoo!"   ,"someString"       , typeof(string)),
                    new MockRecordField(1.345356m   ,"price"            , typeof(decimal)),
                    new MockRecordField(null        ,"maybePrice"       , typeof(decimal)),
                    new MockRecordField(today       ,"date"             , "date", typeof(DateTime)),
                    new MockRecordField(now         ,"datetime"         , typeof(DateTime)),
                });

            var objectFiller = new ObjectFiller();

            var result = (StructWithSingleConstructor)objectFiller.Build(typeof(StructWithSingleConstructor), dataRecord);

            Assert.AreEqual(result.productId, 1);
            Assert.AreEqual(new LongId<Product>(1) as LongId<Product>?, result.nullableProductId);
            Assert.AreEqual(result.someString, "woohoo!");
            Assert.AreEqual(result.price, 1.345356m);
            Assert.AreEqual(result.maybePrice, null);
            Assert.AreEqual(result.date, today);
            Assert.AreEqual(result.datetime, now);
        }

        [TestMethod]
        public void ObjectFiller_TargetTypeIsClassWithMultipleConstructor_ClassIsFilledWithSmallerConstructor()
        {
            var today = Date.Today;
            var now = DateTime.Now;
            var dataRecord = new MockDataReader(
                new List<MockRecordField>()
                {
                    new MockRecordField(1           ,"productId"        , typeof(int)),
                    new MockRecordField("woohoo!"   ,"someString"       , typeof(string)),
                    new MockRecordField(1.345356m   ,"price"            , typeof(decimal)),
                    new MockRecordField(today       ,"date"             , "date", typeof(DateTime)),
                    new MockRecordField(now         ,"datetime"         , typeof(DateTime)),
                });

            var objectFiller = new ObjectFiller();

            var result = (ClassWithMultipleConstructors)objectFiller.Build(typeof(ClassWithMultipleConstructors), dataRecord);

            Assert.AreEqual(result.productId, 1);
            Assert.AreEqual(result.nullableProductId, null);
            Assert.AreEqual(result.someString, "woohoo!");
            Assert.AreEqual(result.price, 1.345356m);
            Assert.AreEqual(result.maybePrice, null);
            Assert.AreEqual(result.date, today);
            Assert.AreEqual(result.datetime, now);
        }

        [TestMethod]
        public void ObjectFiller_TargetTypeIsClassWithMultipleConstructor_ClassIsFilledWithLargerConstructor()
        {
            var today = Date.Today;
            var now = DateTime.Now;
            var dataRecord = new MockDataReader(
                new List<MockRecordField>()
                {
                    new MockRecordField(1           ,"productId"        , typeof(int)),
                    new MockRecordField(1L          ,"nullableProductId", typeof(long)),
                    new MockRecordField("woohoo!"   ,"someString"       , typeof(string)),
                    new MockRecordField(1.345356m   ,"price"            , typeof(decimal)),
                    new MockRecordField(234.345m    ,"maybePrice"       , typeof(decimal)),
                    new MockRecordField(today       ,"date"             , "date", typeof(DateTime)),
                    new MockRecordField(now         ,"datetime"         , typeof(DateTime)),
                });

            var objectFiller = new ObjectFiller();

            var result = (ClassWithMultipleConstructors)objectFiller.Build(typeof(ClassWithMultipleConstructors), dataRecord);

            Assert.AreEqual(result.productId, 1);
            Assert.AreEqual(new LongId<Product>(1) as LongId<Product>?, result.nullableProductId);
            Assert.AreEqual(result.someString, "woohoo!");
            Assert.AreEqual(result.price, 1.345356m);
            Assert.AreEqual(result.maybePrice, 234.345m);
            Assert.AreEqual(result.date, today);
            Assert.AreEqual(result.datetime, now);
        }

        [TestMethod]
        public void ObjectFiller_TargetTypeIsClassWithConstructorsAndAdditionalColumnsAreSelectedForSorting_ClassIsFilledCorrectly()
        {
            var today = Date.Today;
            var now = DateTime.Now;
            var dataRecord = new MockDataReader(
                new List<MockRecordField>()
                {
                    new MockRecordField(1           ,"productId"        , typeof(int)),
                    new MockRecordField("woohoo!"   ,"someString"       , typeof(string)),
                    new MockRecordField(1.345356m   ,"price"            , typeof(decimal)),
                    new MockRecordField(today       ,"date"             , "date", typeof(DateTime)),
                    new MockRecordField(now         ,"datetime"         , typeof(DateTime)),
                    new MockRecordField(now         ,"insertDate"       , typeof(DateTime)),
                    new MockRecordField(now         ,"updateDate"       , typeof(DateTime)),
                });

            var objectFiller = new ObjectFiller();

            var result = (ClassWithMultipleConstructors)objectFiller.Build(typeof(ClassWithMultipleConstructors), dataRecord);

            Assert.AreEqual(result.productId, 1);
            Assert.AreEqual(result.nullableProductId, null);
            Assert.AreEqual(result.someString, "woohoo!");
            Assert.AreEqual(result.price, 1.345356m);
            Assert.AreEqual(result.maybePrice, null);
            Assert.AreEqual(result.date, today);
            Assert.AreEqual(result.datetime, now);
        }

        // helper classes
        public class Product : IId
        {
            public int Id { get; }
            long ILongId.Id { get; }
        }

        public class ClassWithSingleConstructor
        {
            public readonly Id<Product> productId;
            public readonly LongId<Product>? nullableProductId;
            public readonly string someString;
            public readonly decimal price;
            public readonly decimal? maybePrice;
            public readonly Date date;
            public readonly DateTime datetime;

            public ClassWithSingleConstructor(Id<Product> productId, LongId<Product>? nullableProductId, string someString, decimal price, decimal? maybePrice, Date date, DateTime datetime)
            {
                this.productId = productId;
                this.nullableProductId = nullableProductId;
                this.someString = someString;
                this.price = price;
                this.maybePrice = maybePrice;
                this.date = date;
                this.datetime = datetime;
            }
        }

        public class ClassWithMultipleConstructors
        {
            public readonly Id<Product> productId;
            public readonly LongId<Product>? nullableProductId;
            public readonly string someString;
            public readonly decimal price;
            public readonly decimal? maybePrice;
            public readonly Date date;
            public readonly DateTime datetime;

            public ClassWithMultipleConstructors(Id<Product> productId, string someString, decimal price, Date date, DateTime datetime)
            {
                this.productId = productId;
                this.nullableProductId = null;
                this.someString = someString;
                this.price = price;
                this.maybePrice = null;
                this.date = date;
                this.datetime = datetime;
            }

            public ClassWithMultipleConstructors(Id<Product> productId, LongId<Product>? nullableProductId, string someString, decimal price, decimal? maybePrice, Date date, DateTime datetime)
            {
                this.productId = productId;
                this.nullableProductId = nullableProductId;
                this.someString = someString;
                this.price = price;
                this.maybePrice = maybePrice;
                this.date = date;
                this.datetime = datetime;
            }
        }

        public struct StructWithSingleConstructor
        {
            public readonly Id<Product> productId;
            public readonly LongId<Product>? nullableProductId;
            public readonly string someString;
            public readonly decimal price;
            public readonly decimal? maybePrice;
            public readonly Date date;
            public readonly DateTime datetime;

            public StructWithSingleConstructor(Id<Product> productId, LongId<Product>? nullableProductId, string someString, decimal price, decimal? maybePrice, Date date, DateTime datetime)
            {
                this.productId = productId;
                this.nullableProductId = nullableProductId;
                this.someString = someString;
                this.price = price;
                this.maybePrice = maybePrice;
                this.date = date;
                this.datetime = datetime;
            }
        }
    }
}
