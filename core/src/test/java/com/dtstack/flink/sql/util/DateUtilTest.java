package com.dtstack.flink.sql.util;

import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Locale;


public class DateUtilTest {

    @Test
    public void columnToDate(){
        DateUtil.columnToDate("2020-11-21 12:22:11");
        DateUtil.columnToDate(System.currentTimeMillis());
        try{
            DateUtil.columnToDate(1212121);
        }catch (Exception e) {

        }
        DateUtil.columnToDate(new Date(System.currentTimeMillis()));
        DateUtil.columnToDate(new Timestamp(System.currentTimeMillis()));

    }

    @Test
    public void stringToDate()  {
        DateUtil.stringToDate(null);
        DateUtil.stringToDate("2010-01-11 11:32:11");
        DateUtil.stringToDate("2010-01-11");
        DateUtil.stringToDate("11:32:11");
        try{
            DateUtil.stringToDate("xxx:xxx:xx 11:32:11.4444");
        } catch (Exception e){

        }
    }

    @Test
    public void getTodayStart(){
        DateUtil.getTodayStart(System.currentTimeMillis());
    }

    @Test
    public void getTodayStartWithScop(){
        DateUtil.getTodayStart(System.currentTimeMillis()/1000, "MS");
        DateUtil.getTodayStart(System.currentTimeMillis(), "S");
    }

    @Test
    public void getNextDayStart(){
        DateUtil.getNextDayStart(System.currentTimeMillis());
    }


    @Test
    public void getNextDayStartWithScop(){
        DateUtil.getNextDayStart(System.currentTimeMillis()/1000, "MS");
        DateUtil.getNextDayStart(System.currentTimeMillis(), "S");
    }


    @Test
    public void getMonthFirst(){
        DateUtil.getMonthFirst(System.currentTimeMillis());
    }

    @Test
    public void getMonth(){
        DateUtil.getMonth(System.currentTimeMillis());
    }

    @Test
    public void getYear(){
        DateUtil.getYear(System.currentTimeMillis());
    }

    @Test
    public void getWeekFirst(){
        DateUtil.getWeekFirst(System.currentTimeMillis());
    }
    @Test
    public void getWeekOfYear(){
        DateUtil.getWeekOfYear(System.currentTimeMillis());
    }
    @Test
    public void getYesterdayByString(){
        DateUtil.getYesterdayByString("2010-10-11 10:22:31","yyyy-MM-dd HH:mm:ss","yyyy-MM-dd");
    }
    @Test
    public void getTomorrowByString() throws ParseException {
        DateUtil.getTomorrowByString("2010-10-11 10:22:31","yyyy-MM-dd HH:mm:ss","yyyy-MM-dd");
    }


    @Test
    public void getTomorrowByDate() throws ParseException {
        DateUtil.getTomorrowByDate(new java.util.Date());
    }

    @Test
    public void get30DaysBeforeByString() throws ParseException {
        DateUtil.get30DaysBeforeByString("2010-10-11 10:22:31","yyyy-MM-dd HH:mm:ss","yyyy-MM-dd");
    }

    @Test
    public void get30DaysLaterByString() throws ParseException {
        DateUtil.get30DaysLaterByString("2010-10-11 10:22:31","yyyy-MM-dd HH:mm:ss","yyyy-MM-dd");
    }

    @Test
    public void getDateStrToFormat() throws ParseException {
        DateUtil.getDateStrToFormat("2010-10-11 10:22:31","yyyy-MM-dd HH:mm:ss","yyyy-MM-dd");
    }

    @Test
    public void getDateMillToFormat() throws ParseException {
        DateUtil.getDateMillToFormat("2010-10-11 10:22:31","yyyy-MM-dd HH:mm:ss");
    }

    @Test
    public void getFirstDay4Month(){
        DateUtil.getFirstDay4Month(2010, 3);
    }
    @Test
    public void getLastDay4Month(){
        DateUtil.getLastDay4Month(2010, 3);
    }
    @Test
    public void getBeforeMonthDay(){
        DateUtil.getBeforeMonthDay(System.currentTimeMillis(), true);
        DateUtil.getBeforeMonthDay(System.currentTimeMillis(), false);
    }

    @Test
    public void getMillByOneDay(){
        DateUtil.getMillByOneDay();
    }
    @Test
    public void getMillByYesDay(){
        DateUtil.getMillByYesDay();
    }
    @Test
    public void getMillByLastWeekDay(){
        DateUtil.getMillByLastWeekDay();
    }
    @Test
    public void getMillByDay(){
        DateUtil.getMillByDay();
        DateUtil.getMillByDay(1, "-");
        DateUtil.getMillByDay(1, "+");
    }

    @Test
    public void getStampByDay(){
        DateUtil.getStampByDay(1, "-");
        DateUtil.getStampByDay(1, "+");
    }

    @Test
    public void getMillToDay(){
        DateUtil.getMillToDay(Calendar.getInstance(), 1);
    }
    @Test
    public void getStampToDay(){
        DateUtil.getStampToDay(Calendar.getInstance(), 1);
    }
    @Test
    public void getToday(){
        DateUtil.getToday();
    }
    @Test
    public void getDate(){
        DateUtil.getDate(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss");
        DateUtil.getDate(new java.util.Date(), "yyyy-MM-dd HH:mm:ss");
        DateUtil.getDate(new java.util.Date(), "yyyy-MM-dd HH:mm:ss", Locale.getDefault());
    }
    @Test
    public void stringToLong() throws ParseException {
        DateUtil.stringToLong("2010-11-11 12:22:22", "yyyy-MM-dd HH:mm:ss");
    }
    @Test
    public void stringToDateNew() {
        DateUtil.stringToDate("2010-11-11 12:22:22", "yyyy-MM-dd HH:mm:ss");
    }

    @Test
    public void longToString() throws ParseException {
        DateUtil.longToString(System.currentTimeMillis()/1000, "yyyy-MM-dd HH:mm:ss");
    }
    @Test
    public void getMinusDate()  {
        DateUtil.getMinusDate(10, 1);
    }
    @Test
    public void getMillByNow() {
        DateUtil.getMillByNow();
    }
    @Test
    public void getWeeksBetweenTwoDates() {
        DateUtil.getWeeksBetweenTwoDates(System.currentTimeMillis(), System.currentTimeMillis());
    }
    @Test
    public void getMonthsBetweenTwoDates() {
        DateUtil.getMonthsBetweenTwoDates(System.currentTimeMillis(), System.currentTimeMillis());
    }
    @Test
    public void getMaxWeekOfYear() {
        DateUtil.getMaxWeekOfYear(System.currentTimeMillis());
    }
    @Test
    public void parseDate() {
        DateUtil.parseDate("2010-11-11 12:22:22", "yyyy-MM-dd HH:mm:ss");
        DateUtil.parseDate("2010-11-11 12:22:22", "yyyy-MM-dd HH:mm:ss", Locale.getDefault());
    }
    @Test
    public void getMinuteStart() {
        DateUtil.getMinuteStart(System.currentTimeMillis());
    }

    @Test
    public void getHourStart() {
        DateUtil.getHourStart(System.currentTimeMillis());
    }
    @Test
    public void getDateByLong() {
        DateUtil.getDateByLong(System.currentTimeMillis());
    }
    @Test
    public void dateToString() {
        DateUtil.dateToString(new java.util.Date());
    }

    @Test
    public void timestampToString() {
        DateUtil.timestampToString(new java.util.Date());
    }
    @Test
    public void getTimestampFromStr() {
        DateUtil.getTimestampFromStr("2010-02-11T12:22:12.111Z");
    }
    @Test
    public void getDateFromStr() {
        DateUtil.getDateFromStr("2010-02-11 12:22:12");
        DateUtil.getDateFromStr("2010-02-11T12:22:12.111Z");
    }

}
