# All values in ug/m3.
# EU: Directive 2008/50/EC limit values (PM2.5 stage II)
# WHO: Air Quality Guidelines 2021

EU_DAILY_LIMIT = {"PM10": 50.0, "SO2": 125.0}
# how many days per year the daily limit may be exceeded
EU_DAILY_ALLOWED_EXCEEDANCES = {"PM10": 35, "SO2": 3}

EU_ANNUAL_LIMIT = {"PM10": 40.0, "PM2.5": 20.0, "NO2": 40.0, "C6H6": 5.0}

WHO_DAILY = {"PM10": 45.0, "PM2.5": 15.0, "NO2": 25.0, "SO2": 40.0}
WHO_ANNUAL = {"PM10": 15.0, "PM2.5": 5.0, "NO2": 10.0}

# a day counts if at least 75% of hours are there (18/24), same rule for the year
MIN_HOURS_PER_DAY = 18
MIN_ANNUAL_COVERAGE = 0.75
