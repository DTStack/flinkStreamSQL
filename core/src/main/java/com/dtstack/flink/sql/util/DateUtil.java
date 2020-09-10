/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package com.dtstack.flink.sql.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;


/**
 * 
 * 日期工具
 * Date: 2017年03月10日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class DateUtil {

    static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final Pattern DATETIME = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3,9})?Z$");
    private static final Pattern DATE = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}$");
    private static final int MILLIS_PER_SECOND = 1000;



    public static java.sql.Date columnToDate(Object column) {
        if(column instanceof String) {
            return new java.sql.Date(stringToDate((String)column).getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new java.sql.Date(rawData.longValue());
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new java.sql.Date(rawData.longValue());
        } else if (column instanceof java.sql.Date) {
            return (java.sql.Date) column;
        } else if(column instanceof Timestamp) {
            Timestamp ts = (Timestamp) column;
            return new java.sql.Date(ts.getTime());
        }
        throw new IllegalArgumentException("Can't convert " + column.getClass().getName() + " to Date");
    }

    public static Date stringToDate(String strDate)  {
        if(strDate == null){
            return null;
        }
        try {
            return localDateTimetoDate(LocalDateTime.parse(strDate, DATE_TIME_FORMATTER));
        } catch (DateTimeParseException ignored) {
        }

        try {
            return localDateTimetoDate(LocalDate.parse(strDate, DATE_FORMATTER).atStartOfDay());
        } catch (DateTimeParseException ignored) {
        }

        try {
            return localDateTimetoDate(LocalDateTime.of(LocalDate.now(), LocalTime.parse(strDate, TIME_FORMATTER)));
        } catch (DateTimeParseException ignored) {
        }

        throw new RuntimeException("can't parse date");
    }

    public static Date localDateTimetoDate(LocalDateTime localDateTime){
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static LocalDateTime dateToLocalDateTime(Date date){
        date = transformSqlDateToUtilDate(date);
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * 将java.sql.Date 转化为 java.util.Date
     * @param date 不知道是java.sql.Date 还是 java.util.Date
     * @return 最后返回 java.util.Date
     */
    public  static Date transformSqlDateToUtilDate(Date date) {
        if (date instanceof java.sql.Date) {
            date = new Date(date.getTime());
        }
        return date;
    }

    /**
     *
     * 
     * @param day Long 时间
     * @return long
     */
    public static long getTodayStart(long day) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + day).length() > 10) {
            cal.setTime(new Date(day));
        } else {
            cal.setTime(new Date(day * 1000L));
        }
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000L;
        return firstDay;
    }

    /**
     *
     * @param day Long 时间
     * @param scope
     * @return
     */
    public static long getTodayStart(long day,String scope) {
        if("MS".equals(scope)){
            return getTodayStart(day)*1000L;
        }else if("S".equals(scope)){
            return getTodayStart(day);
        }else{
            return getTodayStart(day);
        }
    }

    /**
     *
     * @param day Long 时间
     * @return long
     */
    public static long getNextDayStart(long day) {
        long daySpanMill = 86400000L;
        long nextDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + day).length() > 10) {
            cal.setTime(new Date(day));
        } else {
            cal.setTime(new Date(day * 1000L));
        }
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        nextDay = (cal.getTimeInMillis() + daySpanMill) / 1000L;
        return nextDay;
    }
    
    /**
     *
     * @param day Long 时间
     * @param scope String 级别<br>"MS"：毫秒级<br>"S":秒级
     * @return
     */
    public static long getNextDayStart(long day,String scope) {
        if("MS".equals(scope)){
            return getNextDayStart(day)*1000L;
        }else if("S".equals(scope)){
            return getNextDayStart(day);
        }else{
            return getNextDayStart(day);
        }
    }


    /**
     *
     * @param day
     * @return
     */
    public static long getMonthFirst(long day) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000L));
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000L;
        return firstDay;
    }

    /**
     * @param day
     * @return
     */
    public static int getMonth(long day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        return cal.get(Calendar.MONTH) + 1;
    }

    /**
     *
     * @author yumo.lck
     */
    public static int getYear(long day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        return cal.get(Calendar.YEAR);
    }

    /**
     *
     * @param day
     * @return
     */
    public static long getWeekFirst(long day) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000L));
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }

    /**
     * 根据某个日期时间戳秒值，获取所在周在一年中是第几周.
     *
     * @param day
     * @return
     */
    public static int getWeekOfYear(long day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000L));
        return cal.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String getYesterdayByString(String day, String inFormat, String outFormat){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
            Date date = sdf.parse(day);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int calendarDay = calendar.get(Calendar.DATE);
            calendar.set(Calendar.DATE, calendarDay - 1);
            String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
            return dayBefore;
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String getTomorrowByString(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay + 1);
        String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
        return dayBefore;
    }

    /**
     *
     * @param date
     * @return Date
     * @throws ParseException
     */
    public static Date getTomorrowByDate(Date date) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay + 1);
        return calendar.getTime();
    }

    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String get30DaysBeforeByString(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay - 30);
        return new SimpleDateFormat(outFormat).format(calendar.getTime());
    }

    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String get30DaysLaterByString(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay + 30);
        String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
        return dayBefore;
    }


    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String getDateStrToFormat(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
        return dayBefore;
    }

    public static long getDateMillToFormat(String day, String inFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.getTimeInMillis()/1000L;
    }

    /**
     *
     * @author sishu.yss
     * @param year
     * @param month
     * @return
     */
    public static long getFirstDay4Month(int year, int month) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000L;
        return firstDay;
    }

    /**
     *
     * @author yumo.lck
     * @param year
     * @param month
     * @return
     */
    public static long getLastDay4Month(int year, int month) {
        long lastDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month);
        //1 represents a zero next month, can be seen as the end of the first day of the month most one day, but the data table on the last day of the zero point on the line
        cal.set(Calendar.DAY_OF_MONTH, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        lastDay = cal.getTimeInMillis() / 1000L;
        return lastDay;
    }

    /**
     *
     * @author yumo.lck
     * @param chooseFirstDay
     */

    public static long getBeforeMonthDay(long day, boolean chooseFirstDay) {
        long chooseDay = 0L;
        int currentMonth = getMonth(day);
        int currentYear = getYear(day);
        if (currentMonth > 1) {
            currentMonth--;
        } else {
            currentYear--;
            currentMonth = 12;
        }
        if (chooseFirstDay) {
            chooseDay = getFirstDay4Month(currentYear, currentMonth);
            return chooseDay;
        } else {
            chooseDay = getLastDay4Month(currentYear, currentMonth);
            return chooseDay;
        }

    }

    /**
     * @return long
     */
    public static long getMillByOneDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis() / 1000L;
    }

    /**
     *
     * @return long
     */
    public static long getMillByYesDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis() / 1000L;
    }

    /**
     *
     * @return
     */
    public static long getMillByLastWeekDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 7);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis() / 1000L;
    }

    /**
     * @return long
     */
    public static long getMillByDay(int severalDays,String condition) {
        int dateT=0;
        Calendar cal = Calendar.getInstance();
        if(condition==null){
            return getMillToDay(cal,dateT);
        }
        if("-".equals(condition)){
            dateT = (cal.get(Calendar.DATE) - severalDays);
            return getMillToDay(cal,dateT);
        }
        if("+".equals(condition)){
            dateT = (cal.get(Calendar.DATE) + severalDays);
            return getMillToDay(cal,dateT);
        }
        return getMillToDay(cal,dateT);
    }

    /**
     * @return long
     */
    public static long getStampByDay(int severalDays,String condition) {
        int dateT=0;
        Calendar cal = Calendar.getInstance();
        if(condition==null){
            return getStampToDay(cal,dateT);
        }
        if("-".equals(condition)){
            dateT = (cal.get(Calendar.DATE) - severalDays);
            return getStampToDay(cal,dateT);
        }
        if("+".equals(condition)){
            dateT = (cal.get(Calendar.DATE) + severalDays);
            return getStampToDay(cal,dateT);
        }
        return getStampToDay(cal,dateT);
    }
    /**
     * @return long
     */
    public static long getMillByDay(){
        return getMillByDay(0,null);
    }

    /**
     * @param cal  Calendar
     * @param dateT Integer
     * @return  long
     */
    public static long getMillToDay(Calendar cal,int dateT){
        if(dateT!=0){
            cal.set(Calendar.DATE, dateT);
        }
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis()/1000L;
    }

    /**
     * @param cal  Calendar
     * @param dateT Integer
     * @return  long
     */
    public static long getStampToDay(Calendar cal,int dateT){
        if(dateT!=0){
            cal.set(Calendar.DATE, dateT);
        }
        return cal.getTimeInMillis();
    }

    public static String getToday() {
        Calendar cal = Calendar.getInstance();
        return cal.get(1) + "年" + cal.get(2) + "月" + cal.get(3) + "日";
    }

    /**
     * @param day
     * @return format time
     */
    public static String getDate(long day, String format) {
        Calendar cal = Calendar.getInstance();
        if (("" + day).length() > 10) {
            cal.setTime(new Date(day));
        } else {
            cal.setTime(new Date(day * 1000L));
        }
        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.format(cal.getTime());
    }

    /**
     *
     * @param  date
     * @return
     */
    public static String getDate(Date date, String format) {
        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.format(date);
    }


    /**
     *
     * @param day
     * @param format
     * @return long
     * @throws ParseException
     */
    public static long stringToLong(String day, String format) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        long date = dateFormat.parse(day).getTime();
        return date;
    }

    /**
     * @param day
     * @param format
     * @return Date
     * @throws ParseException
     */
    public static Date stringToDate(String day, String format)  {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(format);
            Date date = dateFormat.parse(day);
            return date;
        } catch (ParseException e) {
            return new Date();
        }
    }


    /**
     * long型时间戳转为String型
     *
     * @param day 秒
     * @return 格式化后的日期
     * @throws ParseException
     */
    public static String longToString(long day, String format) throws ParseException {
        if (("" + day).length() <= 10){
            day=day*1000L;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        String date = dateFormat.format(day);
        return date;
    }

    /**
     *
     * @param day 秒
     * @param minusDay 需要减掉的天数
     * @return 秒
     */
    public static int getMinusDate(int day, int minusDay) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000L));
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - minusDay);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return (int) cal.getTimeInMillis() / 1000;
    }

    /**
     *
     * @return long
     */
    public static long getMillByNow() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        return cal.getTimeInMillis();
    }

    public static int getWeeksBetweenTwoDates(long startDay, long endDay) {
        int week = getWeekOfYear(endDay) - getWeekOfYear(startDay) + 1;
        if(week<1){
            week = getWeekOfYear(endDay) + getMaxWeekOfYear(startDay) - getWeekOfYear(startDay) + 1;
        }
        return week;
    }

    public static int getMaxWeekOfYear(long startDay) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(startDay * 1000L));
        return cal.getMaximum(Calendar.WEEK_OF_YEAR);
    }

    public static int getMonthsBetweenTwoDates(long startDay, long endDay) {
        int month = DateUtil.getMonth(endDay) - DateUtil.getMonth(startDay) + 1;
        if(month<1){
            month = getMonth(endDay) + 12 - getMonth(startDay) +1;
        }
        return month;
    }

    public static Date parseDate(String dateStr, String pattern){
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern(pattern);
        try {
            return sdf.parse(dateStr);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     *
     * @param time Long 时间
     * @return long
     */
    public static long getMinuteStart(long time) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + time).length() > 10) {
            cal.setTime(new Date(time));
        } else {
            cal.setTime(new Date(time * 1000L));
        }
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }

    /**
     * @param time Long
     * @return long
     */
    public static long getHourStart(long time) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + time).length() > 10) {
            cal.setTime(new Date(time));
        } else {
            cal.setTime(new Date(time * 1000));
        }
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        firstDay = cal.getTimeInMillis() / 1000L;
        return firstDay;
    }

    /**
     * @param time
     * @return Date
     */
    public static Date getDateByLong(long time){
        Date date = new Date();
        date.setTime(time);
        return date;
    }


    public static Date parseDate(String dateStr, String pattern, Locale locale){
        SimpleDateFormat df = new SimpleDateFormat(
                pattern, locale);

        df.setTimeZone(new SimpleTimeZone(0, "GMT"));
        try {
            return df.parse(dateStr);
        } catch (ParseException e) {
            return null;
        }
    }

    public static String getDate(Date date, String format, Locale locale) {
        SimpleDateFormat df = new SimpleDateFormat(
                format, locale);
        df.setTimeZone(new SimpleTimeZone(0, "GMT"));
        return df.format(date);
    }

    public static java.sql.Timestamp columnToTimestamp(Object column) {
        if (column == null) {
            return null;
        } else if(column instanceof String) {
            Date date = stringToDate((String) column);
            return null == date ? null : new java.sql.Timestamp(date.getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new java.sql.Timestamp(rawData.longValue());
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new java.sql.Timestamp(rawData.longValue());
        } else if (column instanceof java.sql.Date) {
            return (java.sql.Timestamp) column;
        } else if(column instanceof Timestamp) {
            return (Timestamp) column;
        } else if(column instanceof Date) {
            Date d = (Date)column;
            return new java.sql.Timestamp(d.getTime());
        }

        throw new IllegalArgumentException("Can't convert " + column.getClass().getName() + " to Date");
    }

    public static String dateToString(Date date) {
        LocalDateTime localDateTime = dateToLocalDateTime(date);
        return localDateTime.format(DATE_FORMATTER);
    }

    public static String timestampToString(Date date) {
        LocalDateTime localDateTime = dateToLocalDateTime(date);
        return localDateTime.format(DATE_TIME_FORMATTER);
    }

    public static Timestamp getTimestampFromStr(String timeStr) {
        if (DATETIME.matcher(timeStr).matches()) {
            Instant instant = Instant.from(ISO_INSTANT.parse(timeStr));
            return new Timestamp(instant.getEpochSecond() * MILLIS_PER_SECOND);
        }
        Date date = stringToDate(timeStr);
        return null == date ? null : new Timestamp(date.getTime());
    }

    public static java.sql.Date getDateFromStr(String dateStr) {
        if (DATE.matcher(dateStr).matches()) {
            Instant instant = LocalDate.parse(dateStr).atTime(LocalTime.of(0, 0, 0, 0)).toInstant(ZoneOffset.UTC);
            int offset = TimeZone.getDefault().getOffset(instant.toEpochMilli());
            return new java.sql.Date(instant.toEpochMilli() - offset);
        } else if (DATETIME.matcher(dateStr).matches()) {
            Instant instant = Instant.from(ISO_INSTANT.parse(dateStr));
            return new java.sql.Date(instant.toEpochMilli());
        }
        Date date = stringToDate(dateStr);
        return null == date ? null : new java.sql.Date(date.getTime());
    }

}
