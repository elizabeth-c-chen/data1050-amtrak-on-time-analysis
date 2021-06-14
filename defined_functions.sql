-- pgFormatter-ignore

CREATE FUNCTION get_all_stop_ids (origin_date date, train_num text, OUT stop_id int, OUT station_code text)
	RETURNS SETOF record
	AS $$
	SELECT
		stop_id,
		station_code
	FROM
		stops_joined
	WHERE
		origin_date = $1
		AND train_num = $2;

$$
LANGUAGE SQL;


CREATE OR REPLACE FUNCTION insert_trip_ids ()
	RETURNS void
	AS $$
DECLARE
	tbl_row dates_trains%rowtype;
	stop_ids record;
BEGIN
	FOR tbl_row IN
        SELECT
            origin_date,
            train_num,
            trip_id
        FROM
            dates_trains LOOP
            DECLARE
           --   stop_ids record := (SELECT stop_id FROM stops_joined fj WHERE fj.origin_date = tbl_row.origin_date AND fj.train_num = tbl_row.train_num);
           --   stop ids record := (SELECT * FROM get_all_stop_ids(tbl_row.origin_date, tbl_row.train_num) 
                sid stops_joined%rowtype;
            BEGIN
                FOR sid IN 
                    SELECT 
                        stop_id 
                    FROM 
                        stops_joined fj 
                    WHERE 
                        fj.origin_date = tbl_row.origin_date 
                        AND fj.train_num = tbl_row.train_num LOOP
                    UPDATE stops_joined
                    SET trip_id = tbl_row.trip_id
                    WHERE stops_joined.stop_id = sid.stop_id;
                END LOOP;
            END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;


