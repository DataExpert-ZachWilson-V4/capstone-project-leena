from datetime import timedelta, date
import fiscalyear
import numpy as np
from fiscalyear import *


def getBusDay(dt_time):
    businessdays = 0
    if dt_time.weekday() >= 5:
        return 0
    for i in range(1, 32):
        try:
            thisdate = datetime.date(dt_time.year, dt_time.month, i)
        except(ValueError):
            break
        if thisdate.weekday() < 5:  # Monday == 0, Sunday == 6
            businessdays += 1
        if thisdate == dt_time:
            return businessdays

    return businessdays


def daterange(date1, date2):
    for n in range(int((date2 - date1).days) + 1):
        yield date1 + timedelta(n)


def isWeekday(dt_time):
    a = 0
    if dt_time.isoweekday() < 6:
        a = 1
    return a

def  getJulianYearMonth(dt):
    if dt.timetuple().tm_yday < 10:
        return  str(dt.year) + " 00" + str(dt.timetuple().tm_yday)
    elif dt.timetuple().tm_yday < 100:
        return str(dt.year) + " 0" + str(dt.timetuple().tm_yday)
    else:
        return str(dt.year) + " " + str(dt.timetuple().tm_yday)

def getFiscalMonth(dt_time):
    month_num = (dt_time.month - 7 + 1) % 12
    if month_num == 0:
        return "12"
    elif month_num < 10:
        return "0" + str((dt_time.month - 7 + 1) % 12)
    else:
        return str((dt_time.month - 7 + 1) % 12)


def getFiscalMonthwithoutZero(dt_time):
    month_num = (dt_time.month - 7 + 1) % 12
    if month_num == 0:
        return "12"
    elif month_num < 10:
        return str((dt_time.month - 7 + 1) % 12)
    else:
        return str((dt_time.month - 7 + 1) % 12)


def isWeekend(dt_time):
    a = 1
    if dt_time.isoweekday() < 6:
        a = 0
    return a


def getFiscalYearDayCount(dt_time):
    if (dt_time.year % 4 == 0 and (dt_time.year % 100 != 0 or dt_time.year % 400 == 0)):
        if (dt_time.timetuple().tm_yday - 182) % 366 == 0:
            return 366
        else:
            return (dt_time.timetuple().tm_yday - 182) % 366
    else:

        if (dt_time.timetuple().tm_yday - 181) % 366 == 0:
            return 365
        else:
            return (dt_time.timetuple().tm_yday - 181) % 365


def getWeekDay(dt_time):
    dict = {'Sunday': 1, "Monday": 2, "Tuesday": 3, "Wednesday": 4, "Thursday": 5, "Friday": 6, "Saturday": 7}
    return dict[dt_time.strftime("%A")]


def getCalendarMonthNumInQtr(dt_time):
    dict = {1: 1, 2: 2, 3: 3, 4: 1, 5: 2, 6: 3, 7: 1, 8: 2, 9: 3, 10: 1, 11: 2, 12: 3}
    return dict[dt_time.month]


def getQuarterDay(dt_time):
    year_day = dt.timetuple().tm_yday

    if (dt_time.year % 4 == 0 and (dt_time.year % 100 != 0 or dt_time.year % 400 == 0)):
        if dt_time.timetuple().tm_mon in range(1, 4):
            return dt_time.timetuple().tm_yday
        elif dt_time.timetuple().tm_mon in range(4, 7):
            return dt_time.timetuple().tm_yday - 91
        elif dt_time.timetuple().tm_mon in range(7, 10):
            return dt_time.timetuple().tm_yday - 182
        elif dt_time.timetuple().tm_mon in range(10, 13):
            return dt_time.timetuple().tm_yday - 274
    else:

        if dt_time.timetuple().tm_mon in range(1, 4):
            return dt_time.timetuple().tm_yday
        elif dt_time.timetuple().tm_mon in range(4, 7):
            return dt_time.timetuple().tm_yday - 90
        elif dt_time.timetuple().tm_mon in range(7, 10):
            return dt_time.timetuple().tm_yday - 181
        elif dt_time.timetuple().tm_mon in range(10, 13):
            return dt_time.timetuple().tm_yday - 273


def getCalendarWeeekNumInYear(dt_time):
    if (dt_time.year % 4 == 0 and (dt_time.year % 100 != 0 or dt_time.year % 400 == 0)):
        return ((int(dt.strftime('%U')) + (datetime.date(dt.year + 1, 1,
                                                         1).weekday() != calendar.SUNDAY)) % 53) + 1  # write a  function that take care of 53%53 value
    else:
        return ((int(dt.strftime('%U')) + (datetime.date(dt.year + 1, 1,
                                                         1).weekday() != calendar.SUNDAY)) % 53)  # write a  function that take care of 53%53 value


def getFiscalQuarterDay(dt_time):
    year_day = dt.timetuple().tm_yday

    if (dt_time.year % 4 == 0 and (dt_time.year % 100 != 0 or dt_time.year % 400 == 0)):
        if dt_time.timetuple().tm_mon in range(1, 4):
            return 3
        elif dt_time.timetuple().tm_mon in range(4, 7):
            return 4
        elif dt_time.timetuple().tm_mon in range(7, 10):
            return 1
        elif dt_time.timetuple().tm_mon in range(10, 13):
            return 2
    else:

        if dt_time.timetuple().tm_mon in range(1, 4):
            return 3
        elif dt_time.timetuple().tm_mon in range(4, 7):
            return 4
        elif dt_time.timetuple().tm_mon in range(7, 10):
            return 1
        elif dt_time.timetuple().tm_mon in range(10, 13):
            return 2


def to_integer(dt_time):
    return 10000 * dt_time.year + 100 * dt_time.month + dt_time.day


def calendarWeekNumberinMonth(dt_time):
    if dt.day % 7 == 0:
        return (dt.day - 1) // 7 + 2
    else:
        return (dt.day - 1) // 7 + 1


start_dt = date(2021, 1, 1)
end_dt = date(2026, 12, 31)
counter = 22281
dy_key = []
dy_nat_db_clnd_dt = []
dy_yyyymmdd_clnd_dt_num = []
dy_mmddyyyy_fmt_clnd_dt = []  # 01-21-1960
dy_yyyymmdd_fmt_clnd_dt = []  # 1960-01-31
dy_yyyymm_fmt_clnd_dt = []  # 1960-01
dy_clnd_yr_num = []
dy_clnd_dy_num_in_yr = []
dy_clnd_dy_num_in_qtr = []
dy_clnd_dy_num_in_mth = []
dy_clnd_dy_num_in_wk = []
dy_clnd_wk_num_in_yr = []
dy_clnd_wk_num_in_mth = []
dy_clnd_mth_num_in_yr = []
dy_clnd_mth_num_in_qtr = []
dy_clnd_qtr_num_in_yr = []
dy_clnd_qtr_nam = []
dy_clnd_dy_nam = []
dy_clnd_dy_abrv_nam = []
dy_clnd_mth_nam = []
dy_clnd_mth_abrv_nam = []
dy_clnd_yr_qtr_concat_nam = []
dy_clnd_yr_qtr_concat_num = []
dy_clnd_fst_dy_of_mth_fl = []
dy_clnd_lst_dy_of_mth_fl = []
dy_clnd_fst_dy_of_qtr_fl = []
dy_clnd_lst_dy_of_qtr_fl = []
dy_clnd_fst_dy_of_yr_fl = []
dy_clnd_lst_dy_of_yr_fl = []
dy_bus_dy_of_wk_num = []
dy_bus_dy_of_mth_num = []
dy_julian_yyyymmdd_fmt_dt = []
dy_julian_dy_num = []
dy_wkend_fl = []
dy_wkend_cnt = []
dy_wkdy_fl = []
dy_wkdy_cnt = []
dy_yyyymmdd_fisc_dt_num = []
dy_mmddyyyy_fmt_fisc_dt = []
dy_yyyymmdd_fmt_fisc_dt = []
dy_yyyymm_fmt_fisc_dt = []
dy_nat_db_fisc_dt = []
dy_fisc_yr_num = []
dy_fisc_yr_nam = []
dy_fisc_yr_rng_nam = []
dy_fisc_qtr_nam = []
dy_fisc_mth_nam = []
dy_fisc_yr_qtr_concat_nam = []
dy_fisc_yr_mth_concat_nam = []
dy_fisc_dy_num_in_yr = []
dy_fisc_dy_num_in_qtr = []
dy_fisc_dy_num_in_mth = []
dy_fisc_mth_num_in_yr = []
dy_fisc_qtr_num_in_yr = []
dy_fisc_fst_dy_of_mth_fl = []
dy_fisc_lst_dy_of_mth_fl = []
dy_fisc_fst_dy_of_qtr_fl = []
dy_fisc_lst_dy_of_qtr_fl = []
dy_fisc_fst_dy_of_yr_fl = []
dy_fisc_lst_dy_of_yr_fl = []
dy_sql_ts = []
dy_crt_ts = []
dy_crt_bt_num = []
dy_upd_ts = []
dy_upd_bt_num = []


for dt in daterange(start_dt, end_dt):
    counter = counter + 1

    dy_key.append(counter)
    dy_nat_db_clnd_dt.append(dt.strftime("%Y-%m-%d 00:00:00"))
    dy_yyyymmdd_clnd_dt_num.append(to_integer(dt))
    dy_mmddyyyy_fmt_clnd_dt.append(dt.strftime("%m-%d-%Y"))
    dy_yyyymmdd_fmt_clnd_dt.append(dt.strftime("%Y-%m-%d"))
    dy_yyyymm_fmt_clnd_dt.append(dt.strftime("%Y-%m"))
    dy_clnd_yr_num.append(dt.year)
    dy_clnd_dy_num_in_yr.append(dt.timetuple().tm_yday)
    dy_clnd_dy_num_in_qtr.append(getQuarterDay(dt))
    dy_clnd_dy_num_in_mth.append(dt.day)
    dy_clnd_dy_num_in_wk.append(getWeekDay(dt))
    dy_clnd_wk_num_in_yr.append(
        datetime.date(dt.year, dt.month, dt.day).isocalendar()[1])  # write a  function that take care of 53%53 value
    dy_clnd_wk_num_in_mth.append(calendarWeekNumberinMonth(dt))
    dy_clnd_mth_num_in_yr.append(dt.month)
    dy_clnd_mth_num_in_qtr.append(getCalendarMonthNumInQtr(dt))
    dy_clnd_qtr_num_in_yr.append(((dt.month - 1) // 3) + 1)
    dy_clnd_qtr_nam.append("Q" + str(((dt.month - 1) // 3) + 1))
    dy_clnd_dy_nam.append(dt.strftime("%A"))
    dy_clnd_dy_abrv_nam.append(dt.strftime("%a"))
    dy_clnd_mth_nam.append(dt.strftime("%B"))
    dy_clnd_mth_abrv_nam.append(dt.strftime("%b"))
    dy_clnd_yr_qtr_concat_nam.append(str(dt.year) + " Q" + str(((dt.month - 1) // 3) + 1))
    dy_clnd_yr_qtr_concat_num.append(str(dt.year) + " 0" + str(((dt.month - 1) // 3) + 1))
    dy_clnd_fst_dy_of_mth_fl.append("")  # all fields are NULL
    dy_clnd_lst_dy_of_mth_fl.append("")  # all fields are NULL
    dy_clnd_fst_dy_of_qtr_fl.append("")  # all fields are NULL
    dy_clnd_lst_dy_of_qtr_fl.append("")  # all fields are NULL
    dy_clnd_fst_dy_of_yr_fl.append("")  # all fields are NULL
    dy_clnd_lst_dy_of_yr_fl.append("")  # all fields are NULL

    dy_bus_dy_of_wk_num.append(dt.isoweekday())
    dy_bus_dy_of_mth_num.append(getBusDay(dt))
    dy_julian_yyyymmdd_fmt_dt.append(getJulianYearMonth(dt))
    dy_julian_dy_num.append(str(dt.timetuple().tm_yday))
    dy_wkend_fl.append("")  # all fields are NULL
    dy_wkend_cnt.append(isWeekend(dt))
    dy_wkdy_fl.append("")  # all fields are NULL
    dy_wkdy_cnt.append(isWeekday(dt))

    dy_yyyymmdd_fisc_dt_num.append(to_integer(dt))
    dy_mmddyyyy_fmt_fisc_dt.append(dt.strftime("%m-%d-%Y"))
    dy_yyyymmdd_fmt_fisc_dt.append(dt.strftime("%Y-%m-%d"))
    dy_yyyymm_fmt_fisc_dt.append(dt.strftime("%Y-%m"))
    dy_nat_db_fisc_dt.append("")  # all fields are NULL
    dy_fisc_yr_num.append(FiscalDate(dt.year, dt.month, dt.day).fiscal_year)
    dy_fisc_yr_nam.append("FY" + str(FiscalDate(dt.year, dt.month, dt.day).fiscal_year)[2:])
    dy_fisc_yr_rng_nam.append(str(FiscalDate(dt.year, dt.month, dt.day).prev_fiscal_year)[2:] + "-" + str(
        FiscalDate(dt.year, dt.month, dt.day).fiscal_year)[2:])
    dy_fisc_qtr_nam.append("Q" + str(getFiscalQuarterDay(dt)))
    dy_fisc_mth_nam.append("M" + getFiscalMonth(dt))
    dy_fisc_yr_qtr_concat_nam.append(
        "FY" + str(FiscalDate(dt.year, dt.month, dt.day).fiscal_year)[2:] + " Q" + str(getFiscalQuarterDay(dt)))
    dy_fisc_yr_mth_concat_nam.append(
        "FY" + str(FiscalDate(dt.year, dt.month, dt.day).fiscal_year)[2:] + " M" + getFiscalMonth(dt))
    dy_fisc_dy_num_in_yr.append(getFiscalYearDayCount(dt))
    dy_fisc_dy_num_in_qtr.append(getQuarterDay(dt))
    dy_fisc_dy_num_in_mth.append(dt.day)
    dy_fisc_mth_num_in_yr.append(getFiscalMonthwithoutZero(dt))
    dy_fisc_qtr_num_in_yr.append(str(getFiscalQuarterDay(dt)))

    dy_fisc_fst_dy_of_mth_fl.append("")
    dy_fisc_lst_dy_of_mth_fl.append("")
    dy_fisc_fst_dy_of_qtr_fl.append("")
    dy_fisc_lst_dy_of_qtr_fl.append("")
    dy_fisc_fst_dy_of_yr_fl.append("")
    dy_fisc_lst_dy_of_yr_fl.append("")
    dy_sql_ts.append(dt.strftime("%Y-%m-%d 00:00:00"))
    dy_crt_ts.append("2017-08-01 13:38:55")
    dy_crt_bt_num.append("2")
    dy_upd_ts.append("2017-08-01 13:38:55")
    dy_upd_bt_num.append("2")

import csv

with open('/Users/leenapatil/PycharmProjects/PythonForEverybody/data/day_d_2021.csv', 'w', newline='') as file:
    writer = csv.writer(file, delimiter='|')

    writer.writerow(
        ["dy_key", "dy_nat_db_clnd_dt", "dy_yyyymmdd_clnd_dt_num", "dy_mmddyyyy_fmt_clnd_dt", "dy_yyyymmdd_fmt_clnd_dt",
         "dy_yyyymm_fmt_clnd_dt", "dy_clnd_yr_num",
         "dy_clnd_dy_num_in_yr", "dy_clnd_dy_num_in_qtr", "dy_clnd_dy_num_in_mth", "dy_clnd_dy_num_in_wk",
         "dy_clnd_wk_num_in_yr", "dy_clnd_wk_num_in_mth", "dy_clnd_mth_num_in_yr",
         "dy_clnd_mth_num_in_qtr", "dy_clnd_qtr_num_in_yr", "dy_clnd_qtr_nam", "dy_clnd_dy_nam", "dy_clnd_dy_abrv_nam",
         "dy_clnd_mth_nam", "dy_clnd_mth_abrv_nam", "dy_clnd_yr_qtr_concat_nam", "dy_clnd_yr_qtr_concat_num",
         "dy_clnd_fst_dy_of_mth_fl", "dy_clnd_lst_dy_of_mth_fl", "dy_clnd_fst_dy_of_qtr_fl", "dy_clnd_lst_dy_of_qtr_fl",
         "dy_clnd_fst_dy_of_yr_fl",
         "dy_clnd_lst_dy_of_yr_fl", "dy_bus_dy_of_wk_num", "dy_bus_dy_of_mth_num", "dy_julian_yyyymmdd_fmt_dt",
         "dy_julian_dy_num", "dy_wkend_fl", "dy_wkend_cnt", "dy_wkdy_fl", "dy_wkdy_cnt",
         "dy_yyyymmdd_fisc_dt_num", "dy_mmddyyyy_fmt_fisc_dt", "dy_yyyymmdd_fmt_fisc_dt", "dy_yyyymm_fmt_fisc_dt",
         "dy_nat_db_fisc_dt", "dy_fisc_yr_num", "dy_fisc_yr_nam", "dy_fisc_yr_rng_nam", "dy_fisc_qtr_nam",
         "dy_fisc_mth_nam", "dy_fisc_yr_qtr_concat_nam", "dy_fisc_dy_num_in_yr", "dy_fisc_dy_num_in_qtr",
         "dy_fisc_dy_num_in_mth", "dy_fisc_mth_num_in_yr",
         "dy_fisc_qtr_num_in_yr", "dy_fisc_fst_dy_of_mth_fl", "dy_fisc_lst_dy_of_mth_fl", "dy_fisc_fst_dy_of_qtr_fl",
         "dy_fisc_lst_dy_of_qtr_fl", "dy_fisc_fst_dy_of_yr_fl", "dy_fisc_lst_dy_of_yr_fl", "dy_sql_ts","dy_crt_ts","dy_crt_bt_num",
         "dy_upd_ts","dy_upd_bt_num"])

    writer.writerows(
        zip(dy_key, dy_nat_db_clnd_dt, dy_yyyymmdd_clnd_dt_num, dy_mmddyyyy_fmt_clnd_dt, dy_yyyymmdd_fmt_clnd_dt,
            dy_yyyymm_fmt_clnd_dt, dy_clnd_yr_num,
            dy_clnd_dy_num_in_yr, dy_clnd_dy_num_in_qtr, dy_clnd_dy_num_in_mth, dy_clnd_dy_num_in_wk,
            dy_clnd_wk_num_in_yr, dy_clnd_wk_num_in_mth, dy_clnd_mth_num_in_yr,
            dy_clnd_mth_num_in_qtr, dy_clnd_qtr_num_in_yr, dy_clnd_qtr_nam, dy_clnd_dy_nam, dy_clnd_dy_abrv_nam,
            dy_clnd_mth_nam, dy_clnd_mth_abrv_nam, dy_clnd_yr_qtr_concat_nam, dy_clnd_yr_qtr_concat_num,
            dy_clnd_fst_dy_of_mth_fl, dy_clnd_lst_dy_of_mth_fl, dy_clnd_fst_dy_of_qtr_fl, dy_clnd_lst_dy_of_qtr_fl,
            dy_clnd_fst_dy_of_yr_fl, dy_clnd_lst_dy_of_yr_fl,
            dy_bus_dy_of_wk_num, dy_bus_dy_of_mth_num, dy_julian_yyyymmdd_fmt_dt, dy_julian_dy_num, dy_wkend_fl,
            dy_wkend_cnt, dy_wkdy_fl, dy_wkdy_cnt, dy_yyyymmdd_fisc_dt_num,
            dy_mmddyyyy_fmt_fisc_dt, dy_yyyymmdd_fmt_fisc_dt, dy_yyyymm_fmt_fisc_dt, dy_nat_db_fisc_dt, dy_fisc_yr_num,
            dy_fisc_yr_nam, dy_fisc_yr_rng_nam, dy_fisc_qtr_nam, dy_fisc_mth_nam, dy_fisc_yr_qtr_concat_nam,
            dy_fisc_yr_mth_concat_nam,
            dy_fisc_dy_num_in_yr, dy_fisc_dy_num_in_qtr, dy_fisc_dy_num_in_mth, dy_fisc_mth_num_in_yr,
            dy_fisc_qtr_num_in_yr, dy_fisc_fst_dy_of_mth_fl,
            dy_fisc_lst_dy_of_mth_fl, dy_fisc_fst_dy_of_qtr_fl, dy_fisc_lst_dy_of_qtr_fl, dy_fisc_fst_dy_of_yr_fl,
            dy_fisc_lst_dy_of_yr_fl, dy_sql_ts,dy_crt_ts,dy_crt_bt_num,
         dy_upd_ts,dy_upd_bt_num))
