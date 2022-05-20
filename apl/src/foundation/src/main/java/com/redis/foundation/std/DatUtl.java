/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.redis.foundation.std;

import com.redis.foundation.data.Data;
import org.apache.commons.lang3.StringUtils;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * The DatUtl class provides static convenience methods for calculating date/time
 * conversions.
 * <p>
 * The Apache Commons has a number of good utility methods for date/time calculations.
 * http://commons.apache.org/lang/api-2.5/org/apache/commons/lang/time/DateUtils.html
 * </p>
 *
 * @author Al Cole
 * @version 1.0 Nov 27, 2019
 * @since 1.0
 */
public class DatUtl
{
// http://www.coderanch.com/t/410264/java/java/Julian-Gregorian-date-conversion
// http://users.zoominternet.net/~matto/Java/Julian%20Date%20Converter.htm

// Gregorian Calendar adopted Oct. 15, 1582 (2299161)

    public static double HALFSECOND = 0.5;
    public static List<SimpleDateFormat> mDateFormatList = null;
    public static int GREGORIANSTARTDATE = 15 + 31 * (10 + 12 * 1582);

    private DatUtl()
    {
    }

    /**
     * Returns the Julian day number that begins at noon of this day.
     * Positive year signifies A.D., negative year B.C.
     * Remember that the year after 1 B.C. was 1 A.D.
     * <code>
     * System.out.println("Julian date for May 23, 1968 : " + toJulian(new int[]{1968, 5, 23}));
     * </code>
     * <p>
     * Reference: Numerical Recipes in C, 2nd ed., Cambridge University Press 1992
     * </p>
     * @param aYmd Three value integer array (Year, Month, Day).
     * @return The calculated Julian Day value.
     */
    public static double toJulian(int[] aYmd)
    {
        int year = aYmd[0];
        int month = aYmd[1]; // jan=1, feb=2, ...
        int day = aYmd[2];
        int julianYear = year;
        if (year < 0) julianYear++;
        int julianMonth = month;
        if (month > 2)
        {
            julianMonth++;
        }
        else
        {
            julianYear--;
            julianMonth += 13;
        }

        double julian = (java.lang.Math.floor(365.25 * julianYear)
                + java.lang.Math.floor(30.6001 * julianMonth) + day + 1720995.0);
        if (day + 31 * (month + 12 * year) >= GREGORIANSTARTDATE)
        {
            // change over to Gregorian calendar
            int ja = (int) (0.01 * julianYear);
            julian += 2 - ja + (0.25 * ja);
        }
        return java.lang.Math.floor(julian);
    }

    /**
     * Returns the Julian day number that begins at noon of this day.
     * Positive year signifies A.D., negative year B.C.
     * Remember that the year after 1 B.C. was 1 A.D.
     * <code>
     * System.out.println("Julian date for May 23, 1968 : " + toJulian(new Date()));
     * </code>
     * <p>
     * Reference: Numerical Recipes in C, 2nd ed., Cambridge University Press 1992
     * </p>
     * @param aDate The date to convert.
     * @return The calculated Julian Day value.
     */
    public static double toJulian(Date aDate)
    {
        int[] yearMonthDay = new int[3];
        TimeZone timeZone = TimeZone.getDefault();
        Calendar calendarInstance = Calendar.getInstance(timeZone);
        calendarInstance.setTime(aDate);

        yearMonthDay[0] = calendarInstance.get(Calendar.YEAR);
        yearMonthDay[1] = calendarInstance.get(Calendar.MONTH) + 1;
        yearMonthDay[2] = calendarInstance.get(Calendar.DAY_OF_MONTH);

        return toJulian(yearMonthDay);
    }

    /**
     * Converts a Julian day to a calendar date.
     * <p>
     * Reference: Numerical Recipes in C, 2nd ed., Cambridge University Press 1992
     * </p>
     * @param aJulianDay a Julian Day value.
     * @return A three value integer array {Y, M, D}
     */
    public static int[] fromJulian(double aJulianDay)
    {
        int jalpha, ja, jb, jc, jd, je, year, month, day;
        ja = (int) aJulianDay;
        if (ja >= GREGORIANSTARTDATE)
        {
            jalpha = (int) (((ja - 1867216) - 0.25) / 36524.25);
            ja = ja + 1 + jalpha - jalpha / 4;
        }

        jb = ja + 1524;
        jc = (int) (6680.0 + ((jb - 2439870) - 122.1) / 365.25);
        jd = 365 * jc + jc / 4;
        je = (int) ((jb - jd) / 30.6001);
        day = jb - jd - (int) (30.6001 * je);
        month = je - 1;
        if (month > 12) month = month - 12;
        year = jc - 4715;
        if (month > 2) year--;
        if (year <= 0) year--;

        return new int[]{year, month, day};
    }

    /**
     * Attempts to detect the date/time format of the value and create
     * a 'Date' object.
     *
     * @param aDateTimeValue String value.
     * @return Date object if the format is recognized or <i>null</i>.
     */
    public static Date detectCreateDate(String aDateTimeValue)
    {
        Date createDate = null;

        if (StringUtils.isNotEmpty(aDateTimeValue))
        {
            if (mDateFormatList == null)
            {
                mDateFormatList = new ArrayList<SimpleDateFormat>();
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_ISO8601DATETIME_DEFAULT));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_ISO8601DATETIME_NOMILLI));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_ISO8601DATETIME_MILLI2D));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_ISO8601DATETIME_MILLI3D));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_RFC1123_DATE_TIME));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_SQLORACLEDATE_DEFAULT));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_MM_DD_YY_SLASH_DEFAULT));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_SQLISODATE_DEFAULT));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_SQLISOTIME_DEFAULT));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_SQLISODATETIME_DEFAULT));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_DATE_DEFAULT));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_TIME_AMPM));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_TIME_PLAIN));
                mDateFormatList.add(new SimpleDateFormat(Data.FORMAT_TIMESTAMP_PACKED));
            }

            ParsePosition parsePosition = new ParsePosition(0);
            for (SimpleDateFormat sdf : mDateFormatList)
            {
                sdf.setLenient(false);
                createDate = sdf.parse(aDateTimeValue, parsePosition);
                if (createDate != null)
                    break;
            }
        }
        return createDate;
    }

    /**
     * Calculates the number of business days (excluding weekends)
     * between two dates (inclusive).
     * <p>
     * https://stackoverflow.com/questions/4600034/calculate-number-of-weekdays-between-two-dates-in-java
     * </p>
     * @param aStartDate Start date.
     * @param anEndDate End date.
     *
     * @return Number of business days.
     */
    @SuppressWarnings("UnnecessaryLocalVariable")
    public static long calculateBusinessDays(LocalDate aStartDate, LocalDate anEndDate)
    {
        DayOfWeek startWeekDay = aStartDate.getDayOfWeek().getValue() < 6 ? aStartDate.getDayOfWeek() : DayOfWeek.MONDAY;
        DayOfWeek endWeekDay = anEndDate.getDayOfWeek().getValue() < 6 ? anEndDate.getDayOfWeek() : DayOfWeek.FRIDAY;

        long numberOfWeeks = ChronoUnit.DAYS.between(aStartDate, anEndDate) / 7;
        long totalWeekDays = numberOfWeeks * 5 + Math.floorMod(endWeekDay.getValue() - startWeekDay.getValue(), 5);

        return totalWeekDays + 1;
    }

    /**
     * Calculates the number of business days (excluding weekends)
     * between two dates (inclusive).  In addition, this utility
     * method will factor in holidays (e.g. skip them in the count)
     * for the calculation.
     * <p>
     * https://stackoverflow.com/questions/4600034/calculate-number-of-weekdays-between-two-dates-in-java
     * </p>
     *
     * @param aStartDate Start date.
     * @param anEndDate End date.
     * @param aHolidayList List of holidays to skip.
     *
     * @return Number of business days.
     */
    public static long calculateBusinessDays(LocalDate aStartDate, LocalDate anEndDate,
                                             List<LocalDate> aHolidayList)
    {
        long totalBusinessDays = calculateBusinessDays(aStartDate, anEndDate);

        if ((totalBusinessDays > 0) && (aHolidayList != null))
        {
            for (LocalDate holidayDate : aHolidayList)
            {
                if (holidayDate.isEqual(aStartDate))
                    totalBusinessDays--;
                else if (holidayDate.isEqual(anEndDate))
                    totalBusinessDays--;
                else if ((holidayDate.isAfter(aStartDate)) && (holidayDate.isBefore(anEndDate)))
                    totalBusinessDays--;
            }
        }

        return Math.max(totalBusinessDays, 0);
    }
}
