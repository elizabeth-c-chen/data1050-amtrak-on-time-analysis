import psycopg2
import csv
import os

conn = psycopg2.connect(os.environ.get('DATABASE_URL'), sslmode='require')
assert conn is not None, 'need to fix conn!!'

insert_into_station_info = """
                           INSERT INTO
                               station_info (
                                   station_code,
                                   amtrak_station_name,
                                   crew_change,
                                   weather_location_name,
                                   longitude,
                                   latitude,
                                   nb_next_station,
                                   sb_next_station,
                                   nb_mile,
                                   sb_mile,
                                   nb_stop_num,
                                   sb_stop_num,
                                   nb_miles_to_next,
                                   sb_miles_to_next

                             )
                           VALUES
                               (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT DO NOTHING;
                           """  

insert_into_stops = """
                    INSERT INTO
                        stops (
                            arrival_or_departure,
                            train_num,
                            station_code,
                            direction,
                            origin_date,
                            origin_year,
                            origin_month,
                            origin_week_day,
                            full_sched_arr_dep_datetime,
                            sched_arr_dep_date,
                            sched_arr_dep_week_day,
                            sched_arr_dep_time,
                            act_arr_dep_time,
                            full_act_arr_dep_datetime,
                            timedelta_from_sched,
                            service_disruption,
                            cancellations
                          )
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """

insert_into_weather = """
                      INSERT INTO
                          weather_hourly (
                              location,
                              obs_datetime,
                              temperature,
                              precipitation,
                              cloud_cover,
                              weather_type
                      )
                      VALUES
                          (%s, %s, %s, %s, %s, %s)
                      ON CONFLICT DO NOTHING;
                      """


def execute_command(conn, command):
    """
    Execute specified command in PostgreSQL database.
    """
    try:
        cur = conn.cursor()
        cur.execute(command)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()


def update_table(conn, command, csv_file):
    """
    Insert rows from a CSV file into table specified by the command.
    """
    cur = conn.cursor()
    with open(csv_file, newline='') as file:
        info_reader = csv.reader(file, delimiter=',')
        next(info_reader)  # Skip header
        for row in info_reader:
            try:
                cur.execute(command, tuple(row))
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
                conn.rollback()
        conn.commit()


def update_trains(conn, command, arr_or_dep, csv_file):
    """
    Insert rows from trains CSV file into table specified by the command.
    """
    cur = conn.cursor()
    with open(csv_file, newline='') as file:
        info_reader = csv.reader(file, delimiter=',')
        next(info_reader)  #  Skip header
        for row in info_reader:
            try:
                cur.execute(command, tuple([arr_or_dep] + row))
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
                conn.rollback()
        conn.commit()


def insert_into_station_info_command():
    return insert_into_station_info


def insert_into_stops_command():
    return insert_into_stops


def insert_into_weather_command():
    return insert_into_weather


