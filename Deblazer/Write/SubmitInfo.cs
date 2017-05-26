using System;

namespace Dg.Deblazer.Write
{
    public class SubmitInfo
    {
        public int UpdateCount;
        public int InsertCount;
        public int DeleteCount;
        public int OnSubmitCommandCount;
        private int SubmitCount;
        public TimeSpan TransactionDuration;

        public SubmitInfo()
        {
        }

        public SubmitInfo(int updateCount, int insertCount, int deleteCount, int onSubmitCommandCount, TimeSpan transactionDuration)
        {
            UpdateCount = updateCount;
            InsertCount = insertCount;
            DeleteCount = deleteCount;
            OnSubmitCommandCount = onSubmitCommandCount;
            TransactionDuration = transactionDuration;
            SubmitCount = 1;
        }

        public override string ToString()
        {
            return ToString(Environment.NewLine);
        }

        public string ToString(string separator)
        {
            return "Inserted: " + InsertCount
                + separator
                + "Updated: " + UpdateCount
                + separator
                + "Deleted: " + DeleteCount
                + separator
                + "Transaction duration: " + TransactionDuration.TotalMilliseconds + "ms"
                + (SubmitCount > 1 ? (separator + "Submit count: " + SubmitCount) : "");
        }

        public void Add(SubmitInfo submitInfo)
        {
            UpdateCount += submitInfo.UpdateCount;
            InsertCount += submitInfo.InsertCount;
            OnSubmitCommandCount += submitInfo.OnSubmitCommandCount;
            DeleteCount += submitInfo.DeleteCount;
            TransactionDuration += submitInfo.TransactionDuration;
            SubmitCount += submitInfo.SubmitCount;
        }
    }
}