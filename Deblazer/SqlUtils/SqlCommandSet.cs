// This implementation is from Rhino-Tools: http://rhino-tools.svn.sourceforge.net/viewvc/rhino-tools/trunk/commons/Rhino.Commons/ToPublic/SqlCommandSet.cs
// The same code is also used by NHibernate: https://github.com/nhibernate/nhibernate-core/blob/master/src/NHibernate/AdoNet/SqlClientSqlCommandSet.cs
// NHibernates license allows to use the class if we reference the whole assembly (or open source our code), luckily rhino-tools has a better suitable license.

// Copyright (c) 2005 - 2007 Ayende Rahien (ayende@ayende.com)
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
// * Neither the name of Ayende Rahien nor the names of its
// contributors may be used to endorse or promote products derived from this
// software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

using System;
using System.Data.SqlClient;
using System.Reflection;

namespace Dg.Deblazer.SqlUtils
{
    /// <summary>
    /// Expose the batch functionality in ADO.Net 2.0
    /// Microsoft in its wisdom decided to make my life hard and mark it internal.
    /// Through the use of Reflection and some delegates magic, I opened up the functionality.
    ///
    /// There is NO documentation for this, and likely zero support.
    /// Use at your own risk, etc...
    ///
    /// Observable performance benefits are 50%+ when used, so it is really worth it.
    /// </summary>
    // [ThereBeDragons("Not supported by Microsoft, but has major performance boost")] // There are no dragons :-p
    internal class SqlCommandSet : IDisposable
    {
        private static readonly Type sqlCmdSetType;
        private readonly PropGetter<SqlCommand> commandGetter;
        private readonly PropSetter<int> commandTimeoutSetter;
        private readonly PropGetter<SqlConnection> connectionGetter;
        private readonly PropSetter<SqlConnection> connectionSetter;
        private readonly AppendCommand doAppend;
        private readonly DisposeCommand doDispose;
        private readonly ExecuteNonQueryCommand doExecuteNonQuery;
        private readonly object instance;
        private readonly PropSetter<SqlTransaction> transactionSetter;
        private int countOfCommands;

        static SqlCommandSet()
        {
            Assembly sysData = Assembly.Load("System.Data, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089");
            sqlCmdSetType = sysData.GetType("System.Data.SqlClient.SqlCommandSet");

            if (sqlCmdSetType == null)
            {
                throw new InvalidOperationException("Could not find SqlCommandSet!");
            }
        }

        public SqlCommandSet()
        {
            instance = Activator.CreateInstance(sqlCmdSetType, true);
            connectionSetter = (PropSetter<SqlConnection>)
                Delegate.CreateDelegate(typeof(PropSetter<SqlConnection>), instance, "set_Connection");
            transactionSetter = (PropSetter<SqlTransaction>)Delegate.CreateDelegate(typeof(PropSetter<SqlTransaction>), instance, "set_Transaction");
            connectionGetter = (PropGetter<SqlConnection>)Delegate.CreateDelegate(typeof(PropGetter<SqlConnection>), instance, "get_Connection");
            commandGetter = (PropGetter<SqlCommand>)Delegate.CreateDelegate(typeof(PropGetter<SqlCommand>), instance, "get_BatchCommand");
            commandTimeoutSetter = (PropSetter<int>)Delegate.CreateDelegate(typeof(PropSetter<int>), instance, "set_CommandTimeout");
            doAppend = (AppendCommand)Delegate.CreateDelegate(typeof(AppendCommand), instance, "Append");
            doExecuteNonQuery = (ExecuteNonQueryCommand)Delegate.CreateDelegate(typeof(ExecuteNonQueryCommand), instance, "ExecuteNonQuery");
            doDispose = (DisposeCommand)Delegate.CreateDelegate(typeof(DisposeCommand), instance, "Dispose");
        }

        /// <summary>
        /// Return the batch command to be executed
        /// </summary>
        public SqlCommand BatchCommand
        {
            get { return commandGetter(); }
        }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// The time in seconds to wait for the command to execute.
        /// </summary>
        public int CommandTimeout
        {
            set { commandTimeoutSetter(value); }
        }

        /// <summary>
        /// The number of commands batched in this instance
        /// </summary>
        public int CountOfCommands
        {
            get { return countOfCommands; }
        }

        public SqlConnection Connection
        {
            get { return connectionGetter(); }

            set { connectionSetter(value); }
        }

        public SqlTransaction Transaction
        {
            set { transactionSetter(value); }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            doDispose();
        }

        private delegate void AppendCommand(SqlCommand command);

        private delegate void DisposeCommand();

        private delegate int ExecuteNonQueryCommand();

        private delegate T PropGetter<out T>();

        private delegate void PropSetter<in T>(T item);

        /// <summary>
        /// Append a command to the batch
        /// </summary>
        /// <param name="command"></param>
        public void Append(SqlCommand command)
        {
            AssertHasParameters(command);
            doAppend(command);
            countOfCommands++;
        }

        /// <summary>
        /// This is required because SqlClient.SqlCommandSet will throw if
        /// the command has no parameters.
        /// </summary>
        /// <param name="command"></param>
        private static void AssertHasParameters(SqlCommand command)
        {
            if (command.Parameters.Count == 0)
            {
                throw new ArgumentException("A command in SqlCommandSet must have parameters. You can't pass hardcoded sql strings.");
            }
        }

        /// <summary>
        /// Executes the batch
        /// </summary>
        /// <returns>
        /// This seems to be returning the total number of affected rows in all queries
        /// </returns>
        public int ExecuteNonQuery()
        {
            if (Connection == null)
            {
                throw new ArgumentNullException("Connection");
            }

            if (CountOfCommands == 0)
            {
                return 0;
            }

            return doExecuteNonQuery();
        }
    }
}