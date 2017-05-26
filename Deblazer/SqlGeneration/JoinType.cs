using System;

namespace Dg.Deblazer.SqlGeneration
{
    public enum JoinType
    {
        Inner,
        Full,
        Left,
        Right
    }

    public static class JoinTypeExtensions
    {
        public static string GetJoinString(this JoinType joinType)
        {
            switch (joinType)
            {
                case JoinType.Inner:
                    return "INNER JOIN";

                case JoinType.Left:
                    return "LEFT JOIN";

                case JoinType.Full:
                    return "FULL JOIN";

                case JoinType.Right:
                    return "RIGHT JOIN";

                default:
                    throw new NotImplementedException("JoinType " + (int)joinType + " is not implemented.");
            }
        }
    }
}
