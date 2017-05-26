using System.ComponentModel;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace System
{
    [StructLayout(LayoutKind.Auto)]
    [Serializable]
    [TypeConverter(typeof(DateConverter))]
    public struct Date : IComparable, IFormattable, IConvertible, ISerializable, IComparable<Date>, IEquatable<Date>
    {
        // Number of 100ns ticks per time unit
        private const long TicksPerMillisecond = 10000;

        private const long TicksPerDay = MillisPerDay * TicksPerMillisecond;

        // Number of milliseconds per time unit
        private const int MillisPerDay = 24 * 60 * 60 * 1000;

        // Number of days in a non-leap year
        private const int DaysPerYear = 365;

        // Number of days in 4 years
        private const int DaysPer4Years = (DaysPerYear * 4) + 1;

        // Number of days in 100 years
        private const int DaysPer100Years = (DaysPer4Years * 25) - 1;

        // Number of days in 400 years
        private const int DaysPer400Years = (DaysPer100Years * 4) + 1;

        // Number of days from 1/1/0001 to 12/31/1600
        private const int DaysTo1601 = DaysPer400Years * 4;

        // Number of days from 1/1/0001 to 12/30/1899
        private const int DaysTo1899 = (DaysPer400Years * 4) + (DaysPer100Years * 3) - 367;

        // Number of days from 1/1/0001 to 12/31/9999
        private const int DaysTo10000 = (DaysPer400Years * 25) - 366;

        internal const long MinTicks = 0;
        internal const long MaxTicks = (DaysTo10000 * TicksPerDay) - 1;
        private const long MaxMillis = (long)DaysTo10000 * MillisPerDay;

        private const long FileTimeOffset = DaysTo1601 * TicksPerDay;
        private const long DoubleDateOffset = DaysTo1899 * TicksPerDay;

        // The minimum OA date is 0100/01/01 (Note it's year 100).
        // The maximum OA date is 9999/12/31
        private const long OADateMinAsTicks = (DaysPer100Years - DaysPerYear) * TicksPerDay;

        // All OA dates must be greater than (not >=) OADateMinAsDouble
        private const double OADateMinAsDouble = -657435.0;

        // All OA dates must be less than (not <=) OADateMaxAsDouble
        private const double OADateMaxAsDouble = 2958466.0;

        private const int DatePartYear = 0;
        private const int DatePartDayOfYear = 1;
        private const int DatePartMonth = 2;
        private const int DatePartDay = 3;

        private const UInt64 TicksMask = 0x3FFFFFFFFFFFFFFF;
        private const UInt64 FlagsMask = 0xC000000000000000;
        private const UInt64 LocalMask = 0x8000000000000000;
        private const Int64 TicksCeiling = 0x4000000000000000;

        private const string TicksField = "ticks";
        private const string DateDataField = "dateData";

        private static readonly int[] DaysToMonth365 =
        {
            0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365
        };

        private static readonly int[] DaysToMonth366 =
        {
            0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366
        };

        public static readonly Date MinValue = new Date(MinTicks);
        public static readonly Date MaxValue = new Date(MaxTicks);

        // The data is stored as an unsigned 64-bit integeter
        //   Bits 01-62: The value of 100-nanosecond ticks where 0 represents 1/1/0001 12:00am, up until the value
        //               12/31/9999 23:59:59.9999999
        //   Bits 63-64: A four-state value that describes the DateKind value of the date time, with a 2nd
        //               value for the rare case where the date time is local, but is in an overlapped daylight
        //               savings time hour and it is in daylight savings time. This allows distinction of these
        //               otherwise ambiguous local times and prevents data loss when round tripping from Local to
        //               UTC time.
        private readonly UInt64 dateData;

        public static explicit operator Date(DateTime dateTime)
        {
            return new Date(dateTime.Date.Ticks);
        }

        public static explicit operator DateTime(Date date)
        {
            return new DateTime(date.Ticks);
        }

        // Compares two Date values, returning an integer that indicates
        // their relationship.
        public static int Compare(Date t1, Date t2)
        {
            Int64 ticks1 = t1.InternalTicks;
            Int64 ticks2 = t2.InternalTicks;
            if (ticks1 > ticks2)
            {
                return 1;
            }

            if (ticks1 < ticks2)
            {
                return -1;
            }

            return 0;
        }

        // Returns the tick count corresponding to the given year, month, and day.
        // Will check the if the parameters are valid.
        private static long DateToTicks(int year, int month, int day)
        {
            if (year >= 1 && year <= 9999 && month >= 1 && month <= 12)
            {
                int[] days = IsLeapYear(year) ? DaysToMonth366 : DaysToMonth365;
                if (day >= 1 && day <= days[month] - days[month - 1])
                {
                    int y = year - 1;
                    int n = (y * 365) + (y / 4) - (y / 100) + (y / 400) + days[month - 1] + day - 1;
                    return n * TicksPerDay;
                }
            }

            throw new ArgumentOutOfRangeException(null, "ArgumentOutOfRange_BadYearMonthDay");
        }

        // Returns the number of days in the month given by the year and
        // month arguments.
        public static int DaysInMonth(int year, int month)
        {
            if (month < 1 || month > 12)
            {
                throw new ArgumentOutOfRangeException("month", "ArgumentOutOfRange_Month");
            }

            // IsLeapYear checks the year argument
            int[] days = IsLeapYear(year) ? DaysToMonth366 : DaysToMonth365;
            return days[month] - days[month - 1];
        }

        // Converts an OLE Date to a tick count.
        // This function is duplicated in COMDate.cpp
        internal static long DoubleDateToTicks(double value)
        {
            if (value >= OADateMaxAsDouble || value <= OADateMinAsDouble)
            {
                throw new ArgumentException("Arg_OleAutDateInvalid");
            }

            long millis = (long)((value * MillisPerDay) + (value >= 0 ? 0.5 : -0.5));
            // The interesting thing here is when you have a value like 12.5 it all positive 12 days and 12 hours from 01/01/1899
            // However if you a value of -12.25 it is minus 12 days but still positive 6 hours, almost as though you meant -11.75 all negative
            // This line below fixes up the millis in the negative case
            if (millis < 0)
            {
                millis -= (millis % MillisPerDay) * 2;
            }

            millis += DoubleDateOffset / TicksPerMillisecond;

            if (millis < 0 || millis >= MaxMillis)
            {
                throw new ArgumentException("Arg_OleAutDateScale");
            }

            return millis * TicksPerMillisecond;
        }

        // Checks if this Date is equal to a given object. Returns
        // true if the given object is a boxed Date and its value
        // is equal to the value of this Date. Returns false
        // otherwise.
        public override bool Equals(Object value)
        {
            if (value is Date)
            {
                return InternalTicks == ((Date)value).InternalTicks;
            }

            return false;
        }

        // Compares two Date values for equality. Returns true if
        // the two Date values are equal, or false if they are
        // not equal.
        public static bool Equals(Date t1, Date t2)
        {
            return t1.InternalTicks == t2.InternalTicks;
        }

        public static Date FromBinary(Int64 dateData)
        {
            if ((dateData & unchecked((Int64)LocalMask)) != 0)
            {
                // Local times need to be adjusted as you move from one time zone to another,
                // just as they are when serializing in text. As such the format for local times
                // changes to store the ticks of the UTC time, but with flags that look like a
                // local date.
                Int64 ticks = dateData & unchecked((Int64)TicksMask);
                // Negative ticks are stored in the top part of the range and should be converted back into a negative number
                if (ticks > TicksCeiling - TicksPerDay)
                {
                    ticks = ticks - TicksCeiling;
                }

                return new Date(ticks);
            }

            return FromBinaryRaw(dateData);
        }

        // A version of ToBinary that uses the real representation and does not adjust local times. This is needed for
        // scenarios where the serialized data must maintain compatability
        internal static Date FromBinaryRaw(Int64 dateData)
        {
            Int64 ticks = dateData & (Int64)TicksMask;
            if (ticks < MinTicks || ticks > MaxTicks)
            {
                throw new ArgumentException("Argument_DateBadBinaryData", "dateData");
            }

            return new Date((UInt64)dateData);
        }

        // Creates a Date from a Windows filetime. A Windows filetime is
        // a long representing the date and time as the number of
        // 100-nanosecond intervals that have elapsed since 1/1/1601 12:00am.
        public static Date FromFileTime(long fileTime)
        {
            return FromFileTimeUtc(fileTime);
        }

        public static Date FromFileTimeUtc(long fileTime)
        {
            if (fileTime < 0 || fileTime > MaxTicks - FileTimeOffset)
            {
                throw new ArgumentOutOfRangeException("fileTime", "ArgumentOutOfRange_FileTimeInvalid");
            }

            // This is the ticks in Universal time for this fileTime.
            long universalTicks = fileTime + FileTimeOffset;
            return new Date(universalTicks);
        }

        // Creates a Date from an OLE Automation Date.
        public static Date FromOaDate(double d)
        {
            return new Date(DoubleDateToTicks(d));
        }

        // Returns a Date representing the current date. The date part
        // of the returned value is the current date, and the time-of-day part of
        // the returned value is zero (midnight).
        public static Date Today
        {
            get { return (Date)DateTime.Today; }
        }

        // Checks whether a given year is a leap year. This method returns true if
        // year is a leap year, or false if not.
        public static bool IsLeapYear(int year)
        {
            if (year < 1 || year > 9999)
            {
                throw new ArgumentOutOfRangeException("year", "ArgumentOutOfRange_Year");
            }

            return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        }

        // Constructs a Date from a string. The string must specify a
        // date and optionally a time in a culture-specific or universal format.
        // Leading and trailing whitespace characters are allowed.
        public static Date Parse(String s)
        {
            return (Date)DateTime.Parse(s);
        }

        // Constructs a Date from a string. The string must specify a
        // date and optionally a time in a culture-specific or universal format.
        // Leading and trailing whitespace characters are allowed.
        public static Date Parse(String s, IFormatProvider provider)
        {
            return (Date)DateTime.Parse(s, provider);
        }

        public static Date Parse(String s, IFormatProvider provider, DateTimeStyles styles)
        {
            return (Date)DateTime.Parse(s, provider, styles);
        }

        // Constructs a Date from a string. The string must specify a
        // date and optionally a time in a culture-specific or universal format.
        // Leading and trailing whitespace characters are allowed.
        public static Date ParseExact(String s, string format, IFormatProvider provider)
        {
            return (Date)DateTime.ParseExact(s, format, provider);
        }

        // Constructs a Date from a string. The string must specify a
        // date and optionally a time in a culture-specific or universal format.
        // Leading and trailing whitespace characters are allowed.
        public static Date ParseExact(String s, string format, IFormatProvider provider, DateTimeStyles style)
        {
            return (Date)DateTime.ParseExact(s, format, provider, style);
        }

        public static Date ParseExact(String s, string[] formats, IFormatProvider provider, DateTimeStyles style)
        {
            return (Date)DateTime.ParseExact(s, formats, provider, style);
        }

        public static Boolean TryParse(string s, out Date result)
        {
            DateTime dateTime;
            var b = DateTime.TryParse(s, out dateTime);
            result = (Date)dateTime;
            return b;
        }

        public static Boolean TryParse(string s, IFormatProvider provider, DateTimeStyles styles, out Date result)
        {
            DateTime dateTime;
            var b = DateTime.TryParse(s, provider, styles, out dateTime);
            result = (Date)dateTime;
            return b;
        }

        public static Boolean TryParseExact(string s, string format, IFormatProvider provider, DateTimeStyles style, out Date result)
        {
            DateTime dateTime;
            var b = DateTime.TryParseExact(s, format, provider, style, out dateTime);
            result = (Date)dateTime;
            return b;
        }

        public static Boolean TryParseExact(string s, string[] formats, IFormatProvider provider, DateTimeStyles style, out Date result)
        {
            DateTime dateTime;
            var b = DateTime.TryParseExact(s, formats, provider, style, out dateTime);
            result = (Date)dateTime;
            return b;
        }

        public static Date operator +(Date d, TimeSpan t)
        {
            long ticks = d.InternalTicks;
            long valueTicks = t.Ticks;
            if (valueTicks > MaxTicks - ticks || valueTicks < MinTicks - ticks)
            {
                throw new ArgumentOutOfRangeException("t", "Overflow_DateArithmetic");
            }

            return new Date((UInt64)(ticks + valueTicks) | d.InternalKind);
        }

        public static Date operator -(Date d, TimeSpan t)
        {
            long ticks = d.InternalTicks;
            long valueTicks = t.Ticks;
            if (ticks - MinTicks < valueTicks || ticks - MaxTicks > valueTicks)
            {
                throw new ArgumentOutOfRangeException("t", "Overflow_DateArithmetic");
            }

            return new Date((UInt64)(ticks - valueTicks) | d.InternalKind);
        }

        public static TimeSpan operator -(Date d1, Date d2)
        {
            return new TimeSpan(d1.InternalTicks - d2.InternalTicks);
        }

        public static bool operator ==(Date d1, Date d2)
        {
            return d1.InternalTicks == d2.InternalTicks;
        }

        public static bool operator !=(Date d1, Date d2)
        {
            return d1.InternalTicks != d2.InternalTicks;
        }

        public static bool operator <(Date t1, Date t2)
        {
            return t1.InternalTicks < t2.InternalTicks;
        }

        public static bool operator <=(Date t1, Date t2)
        {
            return t1.InternalTicks <= t2.InternalTicks;
        }

        public static bool operator >(Date t1, Date t2)
        {
            return t1.InternalTicks > t2.InternalTicks;
        }

        public static bool operator >=(Date t1, Date t2)
        {
            return t1.InternalTicks >= t2.InternalTicks;
        }

        // This function is duplicated in COMDate.cpp
        private static double TicksToOaDate(long value)
        {
            if (value == 0)
            {
                return 0.0; // Returns OleAut's zero'ed date value.
            }

            // This is a fix for VB. They want the default day to be 1/1/0001 rathar then 12/30/1899.
            if (value < TicksPerDay) 
            {
                // We could have moved this fix down but we would like to keep the bounds check.
                value += DoubleDateOffset;
            }

            if (value < OADateMinAsTicks)
            {
                throw new OverflowException("Arg_OleAutDateInvalid");
            }

            // Currently, our max date == OA's max date (12/31/9999), so we don't
            // need an overflow check in that direction.
            long millis = (value - DoubleDateOffset) / TicksPerMillisecond;
            if (millis < 0)
            {
                long frac = millis % MillisPerDay;
                if (frac != 0)
                {
                    millis -= (MillisPerDay + frac) * 2;
                }
            }

            return (double)millis / MillisPerDay;
        }

        // Constructs a Date from a tick count. The ticks
        // argument specifies the date as the number of 100-nanosecond intervals
        // that have elapsed since 1/1/0001 12:00am.
        public Date(long ticks)
        {
            if (ticks < MinTicks || ticks > MaxTicks)
            {
                throw new ArgumentOutOfRangeException("ticks", "ArgumentOutOfRange_DateBadTicks");
            }

            dateData = (UInt64)ticks;
        }

        public Date(DateTime dateTime)
        {
            dateData = new Date(dateTime.Year, dateTime.Month, dateTime.Day).dateData;
        }

        private Date(UInt64 dateData)
        {
            this.dateData = dateData;
        }

        // Constructs a Date from a given year, month, and day. The
        // time-of-day of the resulting Date is always midnight.
        public Date(int year, int month, int day)
        {
            dateData = (UInt64)DateToTicks(year, month, day);
        }

        // Constructs a Date from a given year, month, and day for
        // the specified calendar. The
        // time-of-day of the resulting Date is always midnight.
        public Date(int year, int month, int day, Calendar calendar)
        {
            if (calendar == null)
            {
                throw new ArgumentNullException("calendar");
            }

            dateData = (UInt64)calendar.ToDateTime(year, month, day, 0, 0, 0, 0).Ticks;
        }

        // Do not delete StreamingContext context because it is needed to (de)serialize dates! (for [Serializable])
        private Date(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            Boolean foundTicks = false;
            Boolean foundDateData = false;
            Int64 serializedTicks = 0;
            UInt64 serializedDateData = 0;

            // Get the data
            SerializationInfoEnumerator enumerator = info.GetEnumerator();
            while (enumerator.MoveNext())
            {
                switch (enumerator.Name)
                {
                    case TicksField:
                        serializedTicks = Convert.ToInt64(enumerator.Value, CultureInfo.InvariantCulture);
                        foundTicks = true;
                        break;

                    case DateDataField:
                        serializedDateData = Convert.ToUInt64(enumerator.Value, CultureInfo.InvariantCulture);
                        foundDateData = true;
                        break;

                    default:
                        // Ignore other fields for forward compatability.
                        break;
                }
            }

            if (foundDateData)
            {
                dateData = serializedDateData;
            }
            else if (foundTicks)
            {
                dateData = (UInt64)serializedTicks;
            }
            else
            {
                throw new SerializationException("Serialization_MissingDateData");
            }

            Int64 ticks = InternalTicks;
            if (ticks < MinTicks || ticks > MaxTicks)
            {
                throw new SerializationException("Serialization_DateTicksOutOfRange");
            }
        }

        private Int64 InternalTicks
        {
            get { return (Int64)(dateData & TicksMask); }
        }

        private UInt64 InternalKind
        {
            get { return dateData & FlagsMask; }
        }

        // Returns the Date resulting from adding the given
        // TimeSpan to this Date.
        public Date Add(TimeSpan value)
        {
            return AddTicks(value.Ticks);
        }

        // Returns the Date resulting from adding a fractional number of
        // time units to this Date.
        private Date Add(double value, int scale)
        {
            long millis = (long)((value * scale) + (value >= 0 ? 0.5 : -0.5));
            if (millis <= -MaxMillis || millis >= MaxMillis)
            {
                throw new ArgumentOutOfRangeException("value", "ArgumentOutOfRange_AddValue");
            }

            return AddTicks(millis * TicksPerMillisecond);
        }

        // Returns the Date resulting from adding a fractional number of
        // days to this Date. The result is computed by rounding the
        // fractional number of days given by value to the nearest
        // millisecond, and adding that interval to this Date. The
        // value argument is permitted to be negative.
        public Date AddDays(double value)
        {
            return Add(value, MillisPerDay);
        }

        // Returns the Date resulting from adding the given number of
        // months to this Date. The result is computed by incrementing
        // (or decrementing) the year and month parts of this Date by
        // months months, and, if required, adjusting the day part of the
        // resulting date downwards to the last day of the resulting month in the
        // resulting year. The time-of-day part of the result is the same as the
        // time-of-day part of this Date.
        //
        // In more precise terms, considering this Date to be of the
        // form y / m / d + t, where y is the
        // year, m is the month, d is the day, and t is the
        // time-of-day, the result is y1 / m1 / d1 + t,
        // where y1 and m1 are computed by adding months months
        // to y and m, and d1 is the largest value less than
        // or equal to d that denotes a valid day in month m1 of year
        // y1.
        public Date AddMonths(int months)
        {
            if (months < -120000 || months > 120000)
            {
                throw new ArgumentOutOfRangeException("months", "ArgumentOutOfRange_DateBadMonths");
            }

            int y = GetDatePart(DatePartYear);
            int m = GetDatePart(DatePartMonth);
            int d = GetDatePart(DatePartDay);
            int i = m - 1 + months;
            if (i >= 0)
            {
                m = (i % 12) + 1;
                y = y + (i / 12);
            }
            else
            {
                m = 12 + ((i + 1) % 12);
                y = y + ((i - 11) / 12);
            }

            if (y < 1 || y > 9999)
            {
                throw new ArgumentOutOfRangeException("months", "ArgumentOutOfRange_DateArithmetic");
            }

            int days = DaysInMonth(y, m);
            if (d > days)
            {
                d = days;
            }

            return new Date((UInt64)(DateToTicks(y, m, d) + (InternalTicks % TicksPerDay)) | InternalKind);
        }

        // Returns the Date resulting from adding the given number of
        // 100-nanosecond ticks to this Date. The value argument
        // is permitted to be negative.
        public Date AddTicks(long value)
        {
            long ticks = InternalTicks;
            if (value > MaxTicks - ticks || value < MinTicks - ticks)
            {
                throw new ArgumentOutOfRangeException("value", "ArgumentOutOfRange_DateArithmetic");
            }

            return new Date((UInt64)(ticks + value) | InternalKind);
        }

        // Returns the Date resulting from adding the given number of
        // years to this Date. The result is computed by incrementing
        // (or decrementing) the year part of this Date by value
        // years. If the month and day of this Date is 2/29, and if the
        // resulting year is not a leap year, the month and day of the resulting
        // Date becomes 2/28. Otherwise, the month, day, and time-of-day
        // parts of the result are the same as those of this Date.
        public Date AddYears(int value)
        {
            if (value < -10000 || value > 10000)
            {
                throw new ArgumentOutOfRangeException("years", "ArgumentOutOfRange_DateBadYears");
            }

            return AddMonths(value * 12);
        }

        // Compares this Date to a given object. This method provides an
        // implementation of the IComparable interface. The object
        // argument must be another Date, or otherwise an exception
        // occurs.  Null is considered less than any instance.
        //
        // Returns a value less than zero if this  object
        public int CompareTo(object value)
        {
            if (value == null)
            {
                return 1;
            }

            if (!(value is Date))
            {
                throw new ArgumentException("Arg_MustBeDate");
            }

            long valueTicks = ((Date)value).InternalTicks;
            long ticks = InternalTicks;
            if (ticks > valueTicks)
            {
                return 1;
            }

            if (ticks < valueTicks)
            {
                return -1;
            }

            return 0;
        }

        public int CompareTo(Date value)
        {
            long valueTicks = value.InternalTicks;
            long ticks = InternalTicks;
            if (ticks > valueTicks)
            {
                return 1;
            }

            if (ticks < valueTicks)
            {
                return -1;
            }

            return 0;
        }

        public bool Equals(Date value)
        {
            return InternalTicks == value.InternalTicks;
        }

        [SecurityPermission(SecurityAction.LinkDemand, Flags = SecurityPermissionFlag.SerializationFormatter)]
        void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            // Serialize both the old and the new format
            info.AddValue(TicksField, InternalTicks);
            info.AddValue(DateDataField, dateData);
        }

        public Int64 ToBinary()
        {
            return (Int64)dateData;
        }

        // Return the underlying data, without adjust local times to the right time zone. Needed if performance
        // or compatability are important.
        internal Int64 ToBinaryRaw()
        {
            return (Int64)dateData;
        }

        // Returns a given date part of this Date. This method is used
        // to compute the year, day-of-year, month, or day part.
        private int GetDatePart(int part)
        {
            Int64 ticks = InternalTicks;
            // n = number of days since 1/1/0001
            int n = (int)(ticks / TicksPerDay);
            // y400 = number of whole 400-year periods since 1/1/0001
            int y400 = n / DaysPer400Years;
            // n = day number within 400-year period
            n -= y400 * DaysPer400Years;
            // y100 = number of whole 100-year periods within 400-year period
            int y100 = n / DaysPer100Years;
            // Last 100-year period has an extra day, so decrement result if 4
            if (y100 == 4)
            {
                y100 = 3;
            }

            // n = day number within 100-year period
            n -= y100 * DaysPer100Years;
            // y4 = number of whole 4-year periods within 100-year period
            int y4 = n / DaysPer4Years;
            // n = day number within 4-year period
            n -= y4 * DaysPer4Years;
            // y1 = number of whole years within 4-year period
            int y1 = n / DaysPerYear;
            // Last year has an extra day, so decrement result if 4
            if (y1 == 4)
            {
                y1 = 3;
            }

            // If year was requested, compute and return it
            if (part == DatePartYear)
            {
                return (y400 * 400) + (y100 * 100) + (y4 * 4) + y1 + 1;
            }

            // n = day number within year
            n -= y1 * DaysPerYear;
            // If day-of-year was requested, return it
            if (part == DatePartDayOfYear)
            {
                return n + 1;
            }

            // Leap year calculation looks different from IsLeapYear since y1, y4,
            // and y100 are relative to year 1, not year 0
            bool leapYear = y1 == 3 && (y4 != 24 || y100 == 3);
            int[] days = leapYear ? DaysToMonth366 : DaysToMonth365;
            // All months have less than 32 days, so n >> 5 is a good conservative
            // estimate for the month
            int m = (n >> 5) + 1;
            // m = 1-based month number
            while (n >= days[m])
            {
                m++;
            }

            // If month was requested, return it
            if (part == DatePartMonth)
            {
                return m;
            }

            // Return 1-based day-of-month
            return n - days[m - 1] + 1;
        }

        // Returns the day-of-month part of this Date. The returned
        // value is an integer between 1 and 31.
        public int Day
        {
            get { return GetDatePart(DatePartDay); }
        }

        // Returns the day-of-week part of this Date. The returned value
        // is an integer between 0 and 6, where 0 indicates Sunday, 1 indicates
        // Monday, 2 indicates Tuesday, 3 indicates Wednesday, 4 indicates
        // Thursday, 5 indicates Friday, and 6 indicates Saturday.
        public DayOfWeek DayOfWeek
        {
            get { return (DayOfWeek)(((InternalTicks / TicksPerDay) + 1) % 7); }
        }

        // Returns the day-of-year part of this Date. The returned value
        // is an integer between 1 and 366.
        public int DayOfYear
        {
            get { return GetDatePart(DatePartDayOfYear); }
        }

        // Returns the hash code for this Date.
        public override int GetHashCode()
        {
            Int64 ticks = InternalTicks;
            return unchecked((int)ticks) ^ (int)(ticks >> 32);
        }

        // Returns the month part of this Date. The returned value is an
        // integer between 1 and 12.
        public int Month
        {
            get { return GetDatePart(DatePartMonth); }
        }

        // Returns the tick count for this Date. The returned value is
        // the number of 100-nanosecond intervals that have elapsed since 1/1/0001
        // 12:00am.
        public long Ticks
        {
            get { return InternalTicks; }
        }

        // Returns the year part of this Date. The returned value is an
        // integer between 1 and 9999.
        public int Year
        {
            get { return GetDatePart(DatePartYear); }
        }

        public TimeSpan Subtract(Date value)
        {
            return new TimeSpan(InternalTicks - value.InternalTicks);
        }

        public Date Subtract(TimeSpan value)
        {
            long ticks = InternalTicks;
            long valueTicks = value.Ticks;
            if (ticks - MinTicks < valueTicks || ticks - MaxTicks > valueTicks)
            {
                throw new ArgumentOutOfRangeException("value", "ArgumentOutOfRange_DateArithmetic");
            }

            return new Date((UInt64)(ticks - valueTicks) | InternalKind);
        }

        // Converts the Date instance into an OLE Automation compatible
        // double date.
        public double ToOaDate()
        {
            return TicksToOaDate(InternalTicks);
        }

        public long ToFileTime()
        {
            // Treats the input as local if it is not specified
            return ToUniversalTime().ToFileTimeUtc();
        }

        public Date ToUniversalTime()
        {
            return (Date)TimeZone.CurrentTimeZone.ToUniversalTime((DateTime)this);
        }

        public long ToFileTimeUtc()
        {
            // Treats the input as universal if it is not specified
            long ticks = ((InternalKind & LocalMask) != 0) ? ToUniversalTime().InternalTicks : InternalTicks;
            ticks -= FileTimeOffset;
            if (ticks < 0)
            {
                throw new ArgumentOutOfRangeException(null, "ArgumentOutOfRange_FileTimeInvalid");
            }

            return ticks;
        }

        public string ToLongDateString()
        {
            return ((DateTime)this).ToLongDateString();
        }

        public override string ToString()
        {
            return ((DateTime)this).ToShortDateString();
        }

        public string ToString(string format)
        {
            return ((DateTime)this).ToString(format);
        }

        public string ToString(IFormatProvider provider)
        {
            return ((DateTime)this).ToString(provider);
        }

        public string ToString(string format, IFormatProvider provider)
        {
            return ((DateTime)this).ToString(format, provider);
        }

        // Returns a string array containing all of the known date and time options for the
        // current culture.  The strings returned are properly formatted date and
        // time strings for the current instance of Date.
        public string[] GetDateFormats()
        {
            return GetDateFormats(CultureInfo.CurrentCulture);
        }

        // Returns a string array containing all of the known date and time options for the
        // using the information provided by IFormatProvider.  The strings returned are properly formatted date and
        // time strings for the current instance of Date.
        public string[] GetDateFormats(IFormatProvider provider)
        {
            return ((DateTime)this).GetDateTimeFormats(provider);
        }

        // Returns a string array containing all of the date and time options for the
        // given format format and current culture.  The strings returned are properly formatted date and
        // time strings for the current instance of Date.
        public string[] GetDateFormats(char format)
        {
            return ((DateTime)this).GetDateTimeFormats(format);
        }

        // Returns a string array containing all of the date and time options for the
        // given format format and given culture.  The strings returned are properly formatted date and
        // time strings for the current instance of Date.
        public string[] GetDateFormats(char format, IFormatProvider provider)
        {
            return ((DateTime)this).GetDateTimeFormats(format, provider);
        }

        // IValue implementation
        public TypeCode GetTypeCode()
        {
            return TypeCode.Object;
        }

        bool IConvertible.ToBoolean(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "Boolean"));
        }

        char IConvertible.ToChar(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "Char"));
        }

        sbyte IConvertible.ToSByte(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "SByte"));
        }

        byte IConvertible.ToByte(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "Byte"));
        }

        short IConvertible.ToInt16(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "Int16"));
        }

        ushort IConvertible.ToUInt16(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "UInt16"));
        }

        int IConvertible.ToInt32(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "Int32"));
        }
        
        uint IConvertible.ToUInt32(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "UInt32"));
        }

        long IConvertible.ToInt64(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "Int64"));
        }

        ulong IConvertible.ToUInt64(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "UInt64"));
        }

        float IConvertible.ToSingle(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "Single"));
        }

        double IConvertible.ToDouble(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "Double"));
        }

        Decimal IConvertible.ToDecimal(IFormatProvider provider)
        {
            throw new InvalidCastException(String.Format(CultureInfo.CurrentCulture, "Invalid cast from {0} to {1}", "Date", "Decimal"));
        }

        DateTime IConvertible.ToDateTime(IFormatProvider provider)
        {
            return (DateTime)this;
        }

        Object IConvertible.ToType(Type type, IFormatProvider provider)
        {
            return Convert.ChangeType(this, type, provider);
        }

        // Tries to construct a Date from a given year, month, day, hour,
        // minute, second and millisecond.
        internal static Boolean TryCreate(int year, int month, int day, out Date result)
        {
            result = MinValue;
            if (year < 1 || year > 9999 || month < 1 || month > 12)
            {
                return false;
            }

            int[] days = IsLeapYear(year) ? DaysToMonth366 : DaysToMonth365;
            if (day < 1 || day > days[month] - days[month - 1])
            {
                return false;
            }

            long ticks = DateToTicks(year, month, day);

            if (ticks < MinTicks || ticks > MaxTicks)
            {
                return false;
            }

            result = new Date(ticks);
            return true;
        }
    }
}