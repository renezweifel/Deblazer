using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Dg;
using Dg.Deblazer.Utils;
using Dg.Deblazer.Visitors;

namespace Dg.Deblazer.Test.Utils
{
    [TestClass]
    public class DbEntityHierarchyResolverTests
    {
        [TestMethod]
        public void ResolveHierarchy_DbEntityWithoutDependencies_HierarchyEmpty()
        {
            var resolver = new DbEntityHierarchyResolver();

            var hierarchy = resolver.ResolveHierarchy(typeof(EntityWithoutDependencies));

            Assert.AreEqual(0, hierarchy.Count);
        }

        private class EntityWithoutDependencies : TestDbEntity
        {
        }

        [TestMethod]
        public void ResolveHierarchy_DbEntityWithOneDependency_OnePropertyInfoInHierarchy()
        {
            var resolver = new DbEntityHierarchyResolver();

            var hierarchy = resolver.ResolveHierarchy(typeof(EntityALinearDepencency));

            AssertHierarchy(hierarchy, new[] { GetPropertyInfo(typeof(EntityBLinearDependency), "EntityA") });
        }

        [TestMethod]
        public void ResolveHierarchy_DbEntityWithOneDependencyInOppositeDirection_EmptyHierarchy()
        {
            var resolver = new DbEntityHierarchyResolver();

            var hierarchy = resolver.ResolveHierarchy(typeof(EntityBLinearDependency));

            Assert.AreEqual(0, hierarchy.Count);
        }

        /************************************ Linear 1 level ***************************************/
        //             EntityA -> EntityB

        private class EntityALinearDepencency : TestDbEntity
        {
            public int? EntityBId { get; set; }
            public EntityBLinearDependency EntityB { get; set; }
        }

        private class EntityBLinearDependency : TestDbEntity
        {
            public int EntityAId { get; set; }
            public EntityALinearDepencency EntityA { get; set; }
        }

        /***************************************************************************/

        [TestMethod]
        public void ResolveHierarchy_DbEntityWithTwoLevelsOfLinearDependencyFromLevel0_TwoLevelHierarchy()
        {
            var resolver = new DbEntityHierarchyResolver();

            var hierarchy = resolver.ResolveHierarchy(typeof(Level0LinearDependency));

            AssertHierarchy(hierarchy,
                new[] { GetPropertyInfo(typeof(Level1LinearDependency), "Level0") },
                new[] { GetPropertyInfo(typeof(Level2LinearDependency), "Level1") });
        }

        [TestMethod]
        public void ResolveHierarchy_DbEntityWithTwoLevelsOfLinearDependencyFromLevel1_TwoLevelHierarchy()
        {
            var resolver = new DbEntityHierarchyResolver();

            var hierarchy = resolver.ResolveHierarchy(typeof(Level1LinearDependency));

            AssertHierarchy(hierarchy, new[] { GetPropertyInfo(typeof(Level2LinearDependency), "Level1") });
        }

        /************************************ Linear 2 levels ***************************************/
        //             Level0 -> Level1 -> Level2

        private class Level0LinearDependency : TestDbEntity
        {
            public int? Level1Id { get; set; }
            public Level1LinearDependency Level1 { get; set; }
        }

        private class Level1LinearDependency : TestDbEntity
        {
            public int Level0Id { get; set; }
            public Level0LinearDependency Level0 { get; set; }
            public int? Level2Id { get; set; }
            public Level2LinearDependency Level2 { get; set; }
        }

        private class Level2LinearDependency : TestDbEntity
        {
            public int Level1Id { get; set; }
            public Level1LinearDependency Level1 { get; set; }
        }

        /***************************************************************************/

        [TestMethod]
        public void ResolveHierarchy_DbEntityWithTwoLevelTreeHierarchy_TwoLevelHierarchyFirstLevelMerged()
        {
            var resolver = new DbEntityHierarchyResolver();

            var hierarchy = resolver.ResolveHierarchy(typeof(Level0TreeDependency));

            AssertHierarchy(hierarchy,
                new[]
                {
                    GetPropertyInfo(typeof(Level1TreeDependencyA), "Level0"),
                    GetPropertyInfo(typeof(Level1TreeDependencyB), "Level0")
                },
                new[] { GetPropertyInfo(typeof(Level2TreeDependency), "Level1A") });
        }

        /************************************ Asymetric tree ***************************************/
        //             Level0
        //             /    \
        //         Level1A Level1B
        //          /
        //       Level2
        private class Level0TreeDependency : TestDbEntity
        {
            public int? Level1AId { get; set; }
            public Level1TreeDependencyA Level1A { get; set; }
            public int? Level1BId { get; set; }
            public Level1TreeDependencyB Level1B { get; set; }
        }

        private class Level1TreeDependencyA : TestDbEntity
        {
            public int Level0Id { get; set; }
            public Level0TreeDependency Level0 { get; set; }
            public int? Level2Id { get; set; }
            public Level2TreeDependency Level2 { get; set; }
        }

        private class Level1TreeDependencyB : TestDbEntity
        {
            public int Level0Id { get; set; }
            public Level0TreeDependency Level0 { get; set; }
        }

        private class Level2TreeDependency : TestDbEntity
        {
            public int Level1AId { get; set; }
            public Level1TreeDependencyA Level1A { get; set; }
        }

        /***************************************************************************/

        [TestMethod]
        public void ResolveHierarchy_DbEntityWithTwoLevelSymetricTreeHierarchy_TwoLevelHierarchyFirstLevelHasToPropertiesWithSameType()
        {
            var resolver = new DbEntityHierarchyResolver();

            var hierarchy = resolver.ResolveHierarchy(typeof(Level0SymetricTreeDependency));

            AssertHierarchy(hierarchy,
                new[]
                {
                    GetPropertyInfo(typeof(Level1SymetricTreeDependency), "Level0A"),
                    GetPropertyInfo(typeof(Level1SymetricTreeDependency), "Level0B")
                },
                new[] { GetPropertyInfo(typeof(Level2SymetricTreeDependency), "Level1") });
        }

        private void AssertHierarchy(IReadOnlyList<IImmutableSet<PropertyInfo>> hierarchy,
            params PropertyInfo[][] dependencies)
        {
            Assert.AreEqual(dependencies.Length, hierarchy.Count);
            for (var i = 0; i < dependencies.Length; i++)
            {
                AssertDependencies(hierarchy[i], dependencies[i]);
            }
        }

        /************************************ Asymetric tree ***************************************/
        //             Level0
        //             /    \
        //         Level1A Level1B
        //             \    /
        //             Level2
        private class Level0SymetricTreeDependency : TestDbEntity
        {
            public int? Level1AId { get; set; }
            public Level1SymetricTreeDependency Level1A { get; set; }
            public int? Level1BId { get; set; }
            public Level1SymetricTreeDependency Level1B { get; set; }
        }

        private class Level1SymetricTreeDependency : TestDbEntity
        {
            public int Level0AId { get; set; }
            public Level0SymetricTreeDependency Level0A { get; set; }
            public int Level0BId { get; set; }
            public Level0SymetricTreeDependency Level0B { get; set; }
            public int? Level2Id { get; set; }
            public Level2SymetricTreeDependency Level2 { get; set; }
        }

        private class Level2SymetricTreeDependency : TestDbEntity
        {
            public int Level1Id { get; set; }
            public Level1SymetricTreeDependency Level1 { get; set; }
        }

        /***************************************************************************/

        [TestMethod]
        public void ResolveHierarchy_DbEntityWithSimpleOneToNDependency_OnePropertyInfoInHierarchy()
        {
            var resolver = new DbEntityHierarchyResolver();

            var hierarchy = resolver.ResolveHierarchy(typeof(Level0SimpleMultiple));

            AssertHierarchy(hierarchy, new[] { GetPropertyInfo(typeof(Level1SimpleMultiple), "Level0") });
        }

        /************************************ Simple one2n dependency ***************************************/
        //             Level0 -> *Entity1

        private class Level0SimpleMultiple : TestDbEntity
        {
            public IEnumerable<Level1SimpleMultiple> Level1s { get; set; }
        }

        private class Level1SimpleMultiple : TestDbEntity
        {
            public int Level0Id { get; set; }
            public Level0SimpleMultiple Level0 { get; set; }
        }

        /***************************************************************************/

        [TestMethod]
        public void ResolveHierarchy_DbEntityWithAsymetric4LevelTreeWithOneToNDependencyAndMultipleOccurances_3PropertyInfoLevelsInHierarchy()
        {
            var resolver = new DbEntityHierarchyResolver();

            var hierarchy = resolver.ResolveHierarchy(typeof(Level0AsymetricMultiple));

            AssertHierarchy(hierarchy,
                new[]
                {
                    GetPropertyInfo(typeof(Level1AsymetricMultipleA), "Level0"),
                    GetPropertyInfo(typeof(Level1AsymetricMultipleB), "Level0"),
                    GetPropertyInfo(typeof(Level2AsymetricMultipleA), "Level0"),
                },
                new[]
                {
                    GetPropertyInfo(typeof(Level2AsymetricMultipleA), "Level1A"),
                    GetPropertyInfo(typeof(Level2AsymetricMultipleB), "Level1A"),
                    GetPropertyInfo(typeof(Level1AsymetricMultipleB), "Level1A")
                },
                new[]
                {
                    GetPropertyInfo(typeof(Level3AsymetricMultiple), "Level2B"),
                    GetPropertyInfo(typeof(Level0AsymetricMultiple), "Level2B"),
                });
        }

        /********************** Asymetric tree with one2n dependency and multiple occurances and circular dependencies *************************/
        //             (start) Level0   Level1A
        //              /     1/   2\   3/
        //             |   Level1A* Level1B
        //            4|  5/    6\       
        //           Level2A     Level2B 
        //                      7/   8\
        //                  Level3*   Level0 (circular)

        private class Level0AsymetricMultiple : TestDbEntity
        {
            // 1-n dependency (no.1)
            public IEnumerable<Level1AsymetricMultipleA> Level1As { get; set; }

            // siple linear (no. 2)
            public int? Level1BId { get; set; }
            public Level1AsymetricMultipleB Level1B { get; set; }

            // reoccurance (no. 4)
            public int? Level2AId { get; set; }
            public Level2AsymetricMultipleA Level2A { get; set; }

            // circular dependency (no. 8)
            public int Level2BId { get; set; }
            public Level2AsymetricMultipleB Level2B { get; set; }
        }

        private class Level1AsymetricMultipleA : TestDbEntity
        {
            // n-1 (no. 1)
            public int Level0Id { get; set; }
            public Level0AsymetricMultiple Level0 { get; set; }

            // reoccurance (no. 3)
            public int? Level1BId { get; set; }
            public Level1AsymetricMultipleB Level1B { get; set; }


            // linear (no. 5)
            public int? Level2AId { get; set; }
            public Level2AsymetricMultipleA Level2A { get; set; }

            // linear (no. 6)
            public int? Level2BId { get; set; }
            public Level2AsymetricMultipleB Level2B { get; set; }
        }

        private class Level1AsymetricMultipleB : TestDbEntity
        {
            // linear (no. 2)
            public int Level0Id { get; set; }
            public Level0AsymetricMultiple Level0 { get; set; }

            // reoccurance (no. 3)
            public int Level1AId { get; set; }
            public Level1AsymetricMultipleA Level1A { get; set; }
        }

        private class Level2AsymetricMultipleA : TestDbEntity
        {
            // reoccurance (no. 4)
            public int Level0Id { get; set; }
            public Level0AsymetricMultiple Level0 { get; set; }

            // linear (no. 5)
            public int Level1AId { get; set; }
            public Level1AsymetricMultipleA Level1A { get; set; }
        }

        private class Level2AsymetricMultipleB : TestDbEntity
        {
            // linear (no. 6)
            public int Level1AId { get; set; }
            public Level1AsymetricMultipleA Level1A { get; set; }

            // multiple (no. 7)
            public IEnumerable<Level3AsymetricMultiple> Level3s { get; set; }

            // circular (no. 8)
            public int? Level0Id { get; set; }
            public Level0AsymetricMultiple Level0 { get; set; }
        }

        private class Level3AsymetricMultiple : TestDbEntity
        {
            // multiple (no. 7)
            public int Level2BId { get; set; }
            public Level2AsymetricMultipleB Level2B { get; set; }
        }

        /***************************************************************************/

            private static void AssertDependencies(IImmutableSet<PropertyInfo> dependencies, params PropertyInfo[] propertyInfos)
        {
            Assert.AreEqual(propertyInfos.Length, dependencies.Count);
            foreach (var propertyInfo in propertyInfos)
            {
                Assert.IsTrue(dependencies.Contains(propertyInfo));
            }
        }

        private static PropertyInfo GetPropertyInfo(Type entityType, string propertyName)
        {
            var properties = entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var expectedPropertyInfo = properties.Single(pi => pi.Name == propertyName);
            return expectedPropertyInfo;
        }

        private class TestDbEntity : DbEntity
        {
            protected override void CheckProperties(IUpdateVisitor visitor)
            {
                throw new NotImplementedException();
            }

            protected override void HandleChildren(DbEntityVisitorBase visitor)
            {
                throw new NotImplementedException();
            }

            protected override void ModifyInternalState(FillVisitor visitor)
            {
                throw new NotImplementedException();
            }
        }
    }
}
