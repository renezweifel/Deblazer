using System.Collections.Generic;

namespace Dg.Deblazer.Api
{
    /// <summary>
    /// Exposes the enumerator, which supports a simple iteration over a collection of a specified type.
    /// </summary>
    /// <typeparam name="T">The type of objects to enumerate.</typeparam>
    public interface IDbReadOnlyList<T> : IReadOnlyList<T>
    {
        T FirstDb();

        int CountDb(
            [System.Runtime.CompilerServices.CallerFilePath] string callerFilePath = null,
            [System.Runtime.CompilerServices.CallerLineNumber] int callerLineNumber = 0);

        IDbReadOnlyList<T> SkipDb(int elementCount);

        IDbReadOnlyList<T> TakeDb(int elementCount);

        IDbReadOnlyList<T> Clone();
    }
}