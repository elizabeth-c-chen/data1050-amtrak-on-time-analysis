CREATE FUNCTION get_all_stop_ids (origin_date date, train_num text, OUT stop_id int, OUT station_code text)
	RETURNS SETOF record
	AS $$
	SELECT
		stop_id,
		station_code
	FROM
		full_joined
	WHERE
		origin_date = $1
		AND train_num = $2;

$$
LANGUAGE SQL;

SELECT
	d.trip_id,
	-- d.origin_date,
	--	d.train_num,
	CASE WHEN s.stop_id EXISTS THEN
		AS s.BOS_stop_id,
		s.stop_id AS BBY_stop_id,
		s.stop_id AS RTE_stop_id,
		s.stop_id AS PVD_stop_id,
		s.stop_id AS KIN_stop_id,
		s.stop_id AS WLY_stop_id,
		s.stop_id AS MYS_stop_id,
		s.stop_id AS NLC_stop_id,
		s.stop_id AS OSB_stop_id,
		s.stop_id AS NHV_stop_id,
		s.stop_id AS BRP_stop_id,
		s.stop_id AS STM_stop_id,
		s.stop_id AS NRO_stop_id,
		s.stop_id AS NYP_stop_id,
		s.stop_id AS NWK_stop_id,
		s.stop_id AS EWR_stop_id,
		s.stop_id AS MET_stop_id,
		s.stop_id AS TRE_stop_id,
		s.stop_id AS PHL_stop_id,
		s.stop_id AS WIL_stop_id,
		s.stop_id AS ABE_stop_id,
		s.stop_id AS BAL_stop_id,
		s.stop_id AS BWI_stop_id,
		s.stop_id AS NCR_stop_id,
		s.stop_id AS WAS_stop_id
	FROM
		stops s,
		dates_trains d
	WHERE
		BBY_stop_id =
