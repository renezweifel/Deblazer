using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Dg.Deblazer
{
    public class InsertOrderPreservingSet<T> : ISet<T>
    {
        private readonly IDictionary<T, LinkedListNode<T>> dictionary;
        private readonly LinkedList<T> linkedList;

        public InsertOrderPreservingSet()
            : this(EqualityComparer<T>.Default)
        {
        }

        public InsertOrderPreservingSet(IEqualityComparer<T> comparer)
        {
            dictionary = new Dictionary<T, LinkedListNode<T>>(comparer);
            linkedList = new LinkedList<T>();
        }

        public int Count
        {
            get { return dictionary.Count; }
        }

        public virtual bool IsReadOnly
        {
            get { return dictionary.IsReadOnly; }
        }

        void ICollection<T>.Add(T item)
        {
            Add(item);
        }

        public void Clear()
        {
            linkedList.Clear();
            dictionary.Clear();
        }

        public bool Remove(T item)
        {
            LinkedListNode<T> node;
            bool found = dictionary.TryGetValue(item, out node);
            if (!found)
            {
                return false;
            }

            dictionary.Remove(item);
            linkedList.Remove(node);
            return true;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return linkedList.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool Contains(T item)
        {
            return dictionary.ContainsKey(item);
        }

        public void CopyTo(
            T[] array,
            int arrayIndex)
        {
            linkedList.CopyTo(array, arrayIndex);
        }

        public bool Add(T item)
        {
            if (dictionary.ContainsKey(item))
            {
                return false;
            }

            LinkedListNode<T> node = linkedList.AddLast(item);
            dictionary.Add(item, node);
            return true;
        }

        /// <summary>
        ///     Modifies the current set so that it contains all elements that are present in both the current set and in the
        ///     specified collection.
        /// </summary>
        /// <param name="other">The collection to compare to the current set.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public void UnionWith(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            foreach (T element in other)
            {
                Add(element);
            }
        }

        /// <summary>
        ///     Modifies the current set so that it contains only elements that are also in a specified collection.
        /// </summary>
        /// <param name="other">The collection to compare to the current set.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public void IntersectWith(IEnumerable<T> other)
        {
            foreach (T element in other)
            {
                if (Contains(element))
                {
                    continue;
                }

                Remove(element);
            }
        }

        /// <summary>
        ///     Removes all elements in the specified collection from the current set.
        /// </summary>
        /// <param name="other">The collection of items to remove from the set.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public void ExceptWith(IEnumerable<T> other)
        {
            foreach (T element in other)
            {
                Remove(element);
            }
        }

        /// <summary>
        ///     Modifies the current set so that it contains only elements that are present either in the current set or in the
        ///     specified collection, but not both.
        /// </summary>
        /// <param name="other">The collection to compare to the current set.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            foreach (T element in other)
            {
                if (Contains(element))
                {
                    Remove(element);
                }
                else
                {
                    Add(element);
                }
            }
        }

        /// <summary>
        ///     Determines whether a set is a subset of a specified collection.
        /// </summary>
        /// <returns>
        ///     true if the current set is a subset of <paramref name="other" />; otherwise, false.
        /// </returns>
        /// <param name="other">The collection to compare to the current set.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public bool IsSubsetOf(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            var otherHashset = new HashSet<T>(other);
            return otherHashset.IsSupersetOf(this);
        }

        /// <summary>
        ///     Determines whether the current set is a superset of a specified collection.
        /// </summary>
        /// <returns>
        ///     true if the current set is a superset of <paramref name="other" />; otherwise, false.
        /// </returns>
        /// <param name="other">The collection to compare to the current set.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public bool IsSupersetOf(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            return other.All(Contains);
        }

        /// <summary>
        ///     Determines whether the current set is a correct superset of a specified collection.
        /// </summary>
        /// <returns>
        ///     true if the <see cref="T:System.Collections.Generic.ISet`1" /> object is a correct superset of
        ///     <paramref name="other" />; otherwise, false.
        /// </returns>
        /// <param name="other">The collection to compare to the current set. </param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            var otherHashset = new HashSet<T>(other);
            return otherHashset.IsProperSubsetOf(this);
        }

        /// <summary>
        ///     Determines whether the current set is a property (strict) subset of a specified collection.
        /// </summary>
        /// <returns>
        ///     true if the current set is a correct subset of <paramref name="other" />; otherwise, false.
        /// </returns>
        /// <param name="other">The collection to compare to the current set.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            var otherHashset = new HashSet<T>(other);
            return otherHashset.IsProperSupersetOf(this);
        }

        /// <summary>
        ///     Determines whether the current set overlaps with the specified collection.
        /// </summary>
        /// <returns>
        ///     true if the current set and <paramref name="other" /> share at least one common element; otherwise, false.
        /// </returns>
        /// <param name="other">The collection to compare to the current set.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public bool Overlaps(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            if (Count == 0)
            {
                return false;
            }

            return other.Any(Contains);
        }

        /// <summary>
        ///     Determines whether the current set and the specified collection contain the same elements.
        /// </summary>
        /// <returns>
        ///     true if the current set is equal to <paramref name="other" />; otherwise, false.
        /// </returns>
        /// <param name="other">The collection to compare to the current set.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="other" /> is null.</exception>
        public bool SetEquals(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            var otherHashset = new HashSet<T>(other);
            return otherHashset.SetEquals(this);
        }

        // Summary:
        //     Removes all elements that match the conditions defined by the specified predicate
        //     from a System.Collections.Generic.HashSet`1 collection.
        //
        // Parameters:
        //   match:
        //     The System.Predicate`1 delegate that defines the conditions of the elements to
        //     remove.
        //
        // Returns:
        //     The number of elements that were removed from the System.Collections.Generic.HashSet`1
        //     collection.
        //
        // Exceptions:
        //   T:System.ArgumentNullException:
        //     match is null.
        public int RemoveWhere(Predicate<T> match)
        {
            int removeCount = 0;
            foreach (T element in this.ToList())
            {
                if (match(element))
                {
                    removeCount++;
                    Remove(element);
                }
            }

            return removeCount;
        }
    }
}