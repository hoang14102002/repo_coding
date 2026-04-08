/* 
Problem:
Calculate KPIs with multiple metrics for employees and departments. 
The KPI period is defined from the 21st of each month to the 20th of the following month. The results are visualized on Power BI dashboards.

Solution:
Due to the large number of metrics, performing calculations directly in Power BI would be highly complex and inefficient. 
Therefore, a stored procedure was developed to pre-calculate and insert data daily into a dataset table. 
This table serves as the data source for Power BI reports. Additionally, a scheduled event was set up in MySQL to automatically execute the calculations at 8:00 AM every day.
*/

CREATE DEFINER=`abc`@`52.211.32.32` PROCEDURE `analysis_db`.`kpi_report_dataset`(in ngay DATE)
BEGIN
	DECLARE v_ngay DATE;
	DECLARE ngay_dau_ky DATE;
	DECLARE ngay_cuoi_ky DATE;
	DECLARE report_month varchar(50);
	
	DROP TEMPORARY TABLE IF EXISTS tmp_claim_raw;
	DROP TEMPORARY TABLE IF EXISTS tmp_data_dgq;
	DROP TEMPORARY TABLE IF EXISTS tmp_data_dgq_NT;
	DROP TEMPORARY TABLE IF EXISTS tmp_data_ton_lau;
	DROP TEMPORARY TABLE IF EXISTS tmp_vung;

 	DROP TEMPORARY TABLE IF EXISTS tmp_ngay_check_ngay_validate;
 	DROP TEMPORARY TABLE IF EXISTS tmp_approval_log;

	DROP TEMPORARY TABLE IF EXISTS tmp_result;
	DROP TEMPORARY TABLE IF EXISTS tmp_dept_amount;
	DROP TEMPORARY TABLE IF EXISTS tmp_vung_amount;

	DROP TEMPORARY TABLE IF EXISTS tmp_sum_tp; -- bang tinh tong cac chi tieu cho truong phong
	DROP TEMPORARY TABLE IF EXISTS tmp_tp_exist;
	
	SET v_ngay = IFNULL(ngay, CURDATE() - INTERVAL 1 DAY);

	SET report_month = DATE_FORMAT( CASE WHEN DAY(v_ngay) >= 21 THEN DATE_ADD(v_ngay, INTERVAL 1 MONTH) ELSE v_ngay END, '%Y-%m');
    	
	SET ngay_dau_ky = 
		CASE
		    WHEN DAY(v_ngay) >= 21
		        THEN DATE_FORMAT(v_ngay, '%Y-%m-21')
		    ELSE
		        DATE_FORMAT(DATE_SUB(v_ngay, INTERVAL 1 MONTH), '%Y-%m-21')
		END;
	
	SET ngay_cuoi_ky =
    CASE
        WHEN DAY(v_ngay) >= 21
            THEN DATE_FORMAT(DATE_ADD(v_ngay, INTERVAL 1 MONTH), '%Y-%m-20')
        ELSE
            DATE_FORMAT(v_ngay, '%Y-%m-20')
    END;

-- 	/*----------------------------------------------------------------------------------------------------------------*/
	CREATE TEMPORARY TABLE tmp_vung AS
	SELECT department_code, ct_code
	FROM analysis.dim_department
	WHERE unit_code = '000'
	  AND valid = 1 AND ct_code IN ('MB','MN');
-- 	/*----------------------------------------------------------------------------------------------------------------*/
	CREATE TEMPORARY TABLE tmp_claim_raw
	(
	    claim_id BIGINT,
	    claim_number VARCHAR(100),
	    claim_approved_date DATETIME,
	    manage_unit_code VARCHAR(20),
	    tier1_checker_user VARCHAR(50),
	    validate_user VARCHAR(50),
	    system_date DATE,
	    department_code VARCHAR(20),
	    vung varchar(20),
	    call_center_id VARCHAR(100)
	)
	AS
	SELECT
	    t1.claim_id,claim_number,
	    t1.claim_approved_date, t1.manage_unit_code,
	    t1.tier1_checker_user,t1.validate_user,
	    DATE(system_date) AS system_date,
	    SUBSTRING_INDEX(IFNULL(t1.area_code, t1.department_code),'.',1) AS department_code,
	    t2.ct_code as vung, call_center_id
	FROM analysis.factless_claim t1
		LEFT JOIN tmp_vung t2 ON SUBSTRING_INDEX(IFNULL(t1.area_code, t1.department_code),'.',1)= t2.department_code
	WHERE t1.valid = 1 AND t1.motor_category IN ('XE','XEL') AND IFNULL(t1.claim_detail_status,'') <> 1;

 	CREATE INDEX idx_tmp_claim_raw_system_date ON tmp_claim_raw(system_date);
	CREATE INDEX idx_tmp_claim_raw_claim_approved_date ON tmp_claim_raw(claim_approved_date);
 	CREATE INDEX idx_tmp_claim_raw_claim ON tmp_claim_raw(claim_id);
	-- CREATE INDEX idx_tmp_claim_raw_dept ON tmp_claim_raw(department_code);
	/*----------------------------------------------------------------------------------------------------------------*/
	CREATE TEMPORARY TABLE tmp_data_dgq AS
	SELECT
	    t1.claim_id, t1.claim_number,t1.system_date AS system_date,
	    t1.claim_approved_date AS claim_approved_date,
	    t2.business_type_code,t2.coverage_limit,
	    t1.manage_unit_code, t2.compensation_amount AS amount,
	    t1.tier1_checker_user, t1.validate_user,
	    t3.`user` AS employee_code,t1.department_code, t4.ct_code AS vung,
		t5.`level`
	FROM tmp_claim_raw t1
		LEFT JOIN analysis.claim_by_clause t2 ON t1.claim_id = t2.claim_id
		    AND t2.system_date >= CURDATE() - INTERVAL 2 YEAR
		LEFT JOIN analysis.factless_claim t3 ON t1.claim_id = t3.claim_id
		    AND t3.valid = 1 AND t3.product_type IN ('XE','XEL','2BL','2B')
		LEFT JOIN tmp_vung t4 ON t1.department_code = t4.department_code
		LEFT JOIN data_analysis.dim_sos_claim_level_profile t5 ON t2.business_type_code = t5.business_type_code
		    AND t2.compensation_amount BETWEEN t5.amount_from AND t5.amount_to
	WHERE t1.claim_approved_date BETWEEN CONCAT(v_ngay,' 00:00:00') AND CONCAT(v_ngay,' 23:59:59') 
			AND t2.business_type_code NOT LIKE 'XM%';
	/*----------------------------------------------------------------------------------------------------------------*/
	CREATE TEMPORARY TABLE tmp_data_dgq_NT AS
	SELECT
	    t1.claim_id, t1.claim_number,t1.system_date AS system_date,
	    t1.claim_approved_date AS claim_approved_date,
	    t2.business_type_code,t2.coverage_limit,
	    t1.manage_unit_code, t2.compensation_amount AS amount,
	    t1.tier1_checker_user, t1.validate_user,
	    t3.`user` AS employee_code,t1.department_code, t4.ct_code AS vung,
		t5.`level`
	FROM tmp_claim_raw t1
		LEFT JOIN analysis.claim_by_clause t2 ON t1.claim_id = t2.claim_id
		    AND t2.system_date >= CURDATE() - INTERVAL 2 YEAR
		LEFT JOIN analysis.factless_claim t3 ON t1.claim_id = t3.claim_id
		    AND t3.valid = 1 AND t3.product_type IN ('XE','XEL','2BL','2B')
		LEFT JOIN tmp_vung t4 ON t1.department_code = t4.department_code
		LEFT JOIN data_analysis.dim_sos_claim_level_profile t5 ON t2.business_type_code = t5.business_type_code
		    AND t2.compensation_amount BETWEEN t5.amount_from AND t5.amount_to
	WHERE t1.claim_approved_date BETWEEN CONCAT(DATE_SUB(v_ngay, INTERVAL 1 YEAR),' 00:00:00') AND CONCAT(DATE_SUB(v_ngay, INTERVAL 1 YEAR),' 23:59:59') 
			AND t2.business_type_code NOT LIKE 'XM%';
	/*----------------------------------------------------------------------------------------------------------------*/
	CREATE TEMPORARY TABLE tmp_data_ton_lau AS
	SELECT
	    t1.claim_id, t1.claim_number,t1.system_date AS system_date,
	    t1.claim_approved_date AS claim_approved_date,
	    t2.business_type_code,t2.coverage_limit,
	    t1.manage_unit_code, t2.compensation_amount AS amount,
	    t1.tier1_checker_user, t1.validate_user,
	    t3.`user` AS employee_code,t1.department_code, t4.ct_code AS vung,
		t5.`level`
	FROM tmp_claim_raw t1
		LEFT JOIN analysis.claim_by_clause t2 ON t1.claim_id = t2.claim_id
		    AND t2.system_date >= CURDATE() - INTERVAL 2 YEAR
		LEFT JOIN analysis.factless_claim t3 ON t1.claim_id = t3.claim_id
		    AND t3.valid = 1 AND t3.product_type IN ('XE','XEL','2BL','2B')
		LEFT JOIN tmp_vung t4 ON t1.department_code = t4.department_code
		LEFT JOIN data_analysis.dim_sos_claim_level_profile t5 ON t2.business_type_code = t5.business_type_code
		    AND t2.compensation_amount BETWEEN t5.amount_from AND t5.amount_to
	WHERE t1.claim_approved_date > CONCAT(v_ngay,' 23:59:59') 
			AND t2.business_type_code NOT LIKE 'XM%';
-- 	/*----------------------------------------------------------------------------------------------------------------*/
	CREATE TEMPORARY TABLE tmp_ngay_check_ngay_validate AS
	SELECT
	    t1.claim_id,
	    MAX(CASE WHEN t1.action = 'M_CHUYEN_C' THEN t1.created_date END) AS ngay_nhan_check,
	    MAX(CASE WHEN t1.action = 'C_HUYNHAN_M' THEN t1.created_date END) AS ngay_huy_nhan_check,
	    MAX(CASE WHEN t1.action = 'C_CHUYEN_V' THEN t1.created_date END) AS ngay_check,
	    MAX(CASE WHEN t1.action = 'V_HUYNHAN_C' THEN t1.created_date END) AS ngay_huy_nhan_validate,
	    MAX(CASE WHEN t1.action IN ('V_DUYET','V_DUYET_DG_BL') THEN t1.created_date END) AS ngay_validate,
	    t2.department_code,
	    t2.vung AS vung, MAX(t4.level) AS level, SUM(t3.compensation_amount) as amount,
	    MAX(t2.tier1_checker_user)  as tier1_checker_user, MAX(t2.validate_user) as validate_user
	FROM analysis.claim_history t1
		INNER JOIN tmp_claim_raw t2 ON t1.claim_id = t2.claim_id
		LEFT JOIN analysis.claim_by_clause t3 ON t1.claim_id = t3.claim_id
		LEFT JOIN data_analysis.dim_sos_claim_level_profile t4 ON t3.business_type_code = t4.business_type_code
		    AND t3.compensation_amount BETWEEN t4.amount_from AND t4.amount_to
	GROUP BY t1.claim_id, t2.department_code, t2.vung;

	CREATE INDEX idx_tmp_ngay_claim ON tmp_ngay_check_ngay_validate(claim_id);
-- 	/*----------------------------------------------------------------------------------------------------------------*/
	CREATE TEMPORARY TABLE tmp_approval_log AS
	SELECT 
	    t1.claim_id,t1.sent_user,
	    t1.received_user, t1.action, t1.created_date,
	    t1.amount, t1.han_check,
	    t1.han_validate,t1.department_code, t1.vung,
	    working_seconds(
	        LAG(t1.created_date) OVER (PARTITION BY t1.claim_id ORDER BY t1.created_date),
	        t1.created_date
	    ) AS working_seconds_diff
	FROM
	(
	    SELECT t2.claim_id, t2.sent_user,
	        t2.received_user, t2.action,
	        t2.created_date, t3.compensation_amount AS amount,
	        CASE
	            WHEN t3.compensation_amount < 20000000 THEN 7200
	            WHEN t3.compensation_amount <= 50000000 THEN 14400
	            WHEN t3.compensation_amount <= 100000000 THEN 18000
	            ELSE 36000
	        END AS han_check,
	        CASE
	            WHEN t3.compensation_amount <= 50000000 THEN 7200
	            WHEN t3.compensation_amount <= 100000000 THEN 10800
	            ELSE 21600
	        END AS han_validate,
	        t4.department_code,
	        t5.ct_code AS vung,
	        ROW_NUMBER() OVER (
	            PARTITION BY t2.claim_id, t2.action 
	            ORDER BY t2.created_date DESC
	        ) AS rn
	    FROM analysis.claim_history t2
		    INNER JOIN tmp_claim_raw t4 ON t2.claim_id = t4.claim_id
		    INNER JOIN analysis.claim_by_clause t3 ON t2.claim_id = t3.claim_id AND t3.business_type_code = 'XO.4.1.1'
		    LEFT JOIN tmp_vung t5 ON t4.department_code = t5.department_code
	    WHERE t2.action IN ('M_CHUYEN_C','C_CHUYEN_V','V_DUYET','V_DUYET_DG_BL')
	) t1
	WHERE t1.rn = 1;
	/*----------------------------------------------------------------------------------------------------------------*/
	IF DAY(v_ngay) IN (20, 21) THEN	
		DROP TEMPORARY TABLE IF EXISTS tmp_estimate_amount;
	
		CREATE TEMPORARY TABLE tmp_estimate_amount
		(
		    claim_id BIGINT,
		    claim_approved_date DATETIME,
		    employee_code VARCHAR(50),
		    change_date DATE,
		    estimated_amount DECIMAL(30,4),
		    type1 VARCHAR(20),
		    type2 VARCHAR(20),
		    department_code VARCHAR(20),
		    vung VARCHAR(200)
		);
	
		INSERT INTO tmp_estimate_amount(claim_id,claim_approved_date, employee_code, change_date, estimated_amount,type1,type2, department_code,vung)
		SELECT  t1.claim_id,MAX(t1.claim_approved_date) AS claim_approved_date, MAX(t2.`user`),
		    DATE(t3.request_approve_premium_date) AS change_date,
		    SUM(t3.request_amount) AS estimated_amount,'uoc' AS type1,
		    IF(DATE(t3.request_approve_premium_date) < ngay_dau_ky,'DAU_KY', 'CUOI_KY') AS type2,
		    t1.department_code,t4.ct_code AS vung
		FROM tmp_claim_raw t1
			LEFT JOIN analysis.factless_claim t2 ON t1.claim_id = t2.claim_id
			    AND t2.valid = 1 AND t2.product_type IN ('XE','XEL','2BL','2B')
			INNER JOIN analysis.core_fact_estimate_claim_request_amount t3 ON t1.claim_id = t3.claim_id
			    AND t3.request_amount <> 0
			    AND t3.request_approve_premium_date IS NOT NULL
			    AND DATE(t3.request_approve_premium_date) < ngay_cuoi_ky 
			LEFT JOIN tmp_vung t4 ON t1.department_code = t4.department_code
		WHERE t1.system_date < ngay_dau_ky 
			AND t1.claim_approved_date >= CONCAT(ngay_dau_ky,' 00:00:00')
			AND RIGHT(t3.request_approve_premium_date,2) <> '00'
		GROUP BY t1.claim_id, t3.request_approve_premium_date,t1.department_code,t4.ct_code,
			IF(DATE(t3.request_approve_premium_date) < ngay_dau_ky,'DAU_KY', 'CUOI_KY');
		
		INSERT INTO tmp_estimate_amount(claim_id, claim_approved_date, employee_code, change_date, estimated_amount,type1,type2, department_code,vung)
		SELECT t1.claim_id, t1.claim_approved_date,t3.`user`,
		    DATE(t2.eff_date) AS change_date, t2.approval_amount AS estimated_amount, 'baogia' AS type1,
		    IF(DATE(t2.eff_date) < ngay_dau_ky,'DAU_KY','CUOI_KY') AS type2,
		    t1.department_code,
		    t4.ct_code AS vung
		FROM tmp_claim_raw t1
			INNER JOIN analysis.factless_claim t2 ON t1.claim_id = t2.claim_id
			    AND t2.approval_amount <> 0
			    AND DATE(t2.eff_date) < ngay_cuoi_ky
			 LEFT JOIN analysis.factless_claim t3 ON t1.claim_id = t3.claim_id
			    AND t3.valid = 1 AND t3.product_type IN ('XE','XEL','2BL','2B')
			LEFT JOIN tmp_vung t4 ON t1.department_code = t4.department_code
		WHERE t1.system_date < ngay_dau_ky
			AND t1.claim_approved_date >= CONCAT(ngay_dau_ky,' 00:00:00');
		
		DROP TEMPORARY TABLE IF EXISTS tmp_estimate_amount_dau_ky;
	
		CREATE TEMPORARY TABLE tmp_estimate_amount_dau_ky AS
		SELECT *, ROW_NUMBER() OVER(PARTITION BY claim_id, type2 ORDER BY type1,change_date desc) as roww
		FROM tmp_estimate_amount
		WHERE type2 = 'DAU_KY'; 
		
		DROP TEMPORARY TABLE IF EXISTS tmp_estimate_amount_cuoi_ky;
		
		CREATE TEMPORARY TABLE tmp_estimate_amount_cuoi_ky AS
		SELECT *, ROW_NUMBER() OVER(PARTITION BY claim_id, type2 ORDER BY type1,change_date) as roww
		FROM tmp_estimate_amount
		WHERE type2 = 'CUOI_KY';
		
		DROP TEMPORARY TABLE IF EXISTS tmp_estimate_amount_result;
		
		CREATE TEMPORARY TABLE tmp_estimate_amount_result
		(
		    claim_id BIGINT,
		    employee_code VARCHAR(50),
		    department_code VARCHAR(20),
		    vung VARCHAR(200),
		    uoc_dau_ky DECIMAL(30,4),
		    uoc_cuoi_ky DECIMAL(30,4)
		);
	
		INSERT INTO tmp_estimate_amount_result(claim_id, employee_code, department_code, vung, uoc_dau_ky, uoc_cuoi_ky)
		WITH tmp_duyet AS 
		(
			SELECT t1.claim_id, SUM(t2.compensation_amount) AS compensation_amount
			FROM tmp_claim_raw t1 
				INNER JOIN analysis.claim_by_clause t2 ON t1.claim_id = t2.claim_id
			WHERE t1.system_date < ngay_dau_ky
				AND t1.claim_approved_date >= CONCAT(ngay_dau_ky,' 00:00:00')
			GROUP BY t1.claim_id
		)
		SELECT t1.claim_id, t1.employee_code, t1.department_code, t1.vung, t1.estimated_amount AS uoc_dau_ky,
			IF(DATE(t1.claim_approved_date) = '3000-01-01', IFNULL(t2.estimated_amount,t3.compensation_amount), t3.compensation_amount) as uoc_cuoi_ky
		FROM tmp_estimate_amount_dau_ky t1
			LEFT JOIN tmp_estimate_amount_cuoi_ky t2 ON t1.claim_id = t2.claim_id AND t2.roww = 1
			INNER JOIN tmp_duyet t3 ON t1.claim_id = t3.claim_id
		WHERE t1.roww = 1 AND t1.vung IS NOT NULL;
	END IF;
	/*----------------------------------------------------------------------------------------------------------------*/
	CREATE TEMPORARY TABLE tmp_result (
	    vung VARCHAR(100),
	    department_code VARCHAR(100),
	    employee_code VARCHAR(100),
	    ma_chi_tieu VARCHAR(100),
	    ten_chi_tieu VARCHAR(100),
	    value float	    
	);

	-- 	GD: TLBT thuc hien tử số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT DISTINCT t1.vung, t1.department_code, t1.employee_code,
		'GD_TLBT_TU', 'Tỷ lệ BT thực hiện (tử số)', NULL
		-- SUM(t1.amount * t2.claim_rate)
	FROM tmp_data_dgq t1;
-- 		LEFT JOIN data_analysis.dim_sos_claim_rate_by_manage_unit t2 ON t1.manage_unit_code = t2.unit_code
-- 			AND t2.report_month  = report_month
--	GROUP BY t1.vung, t1.department_code, t1.employee_code;
	
	WITH tmp_dept_amount_tu_so AS 
	(
		SELECT t1.department_code, SUM(t1.amount * t2.claim_rate) AS amount
		FROM tmp_data_dgq t1
		LEFT JOIN data_analysis.dim_sos_claim_rate_by_manage_unit t2 ON t1.manage_unit_code = t2.unit_code
			AND t2.report_month  = report_month
		GROUP BY  t1.department_code
	)
	UPDATE tmp_result t1
		LEFT JOIN tmp_dept_amount_tu_so t2 ON t1.department_code = t2.department_code
	SET t1.value = t2.amount
	WHERE t1.value IS NULL AND t1.ma_chi_tieu = 'GD_TLBT_TU';

-- 	GD: TLBT thuc hien mẫu số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECt DISTINCT t1.vung, t1.department_code, t1.employee_code,
		'GD_TLBT_MAU', 'Tỷ lệ BT thực hiện (mẫu số)',NULL
	FROM tmp_data_dgq t1;
	
	WITH tmp_dept_amount_mau_so AS 
	(
		SELECT department_code, SUM(amount) as amount
		FROM tmp_data_dgq
		GROUP BY department_code
	)
	UPDATE tmp_result t1
		LEFT JOIN tmp_dept_amount_mau_so t2 ON t1.department_code = t2.department_code
	SET t1.value = t2.amount
	WHERE t1.value IS NULL AND t1.ma_chi_tieu = 'GD_TLBT_MAU';

-- 	TD: TLBT thuc hien tử số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECt t1.vung, t1.department_code, t1.employee_code,
		'TD_TLBT_TU', 'Tỷ lệ BT thực hiện (tử số)',
		SUM(t1.amount * t2.claim_rate)
	FROM tmp_data_dgq t1
		LEFT JOIN data_analysis.dim_sos_claim_rate_by_manage_unit t2 ON t1.manage_unit_code = t2.unit_code
			AND t2.report_month  = report_month
	GROUP BY t1.vung, t1.department_code, t1.employee_code;
	
-- 	TD: TLBT thuc hien mẫu số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECt DISTINCT t1.vung, t1.department_code, t1.employee_code,
		'TD_TLBT_MAU', 'Tỷ lệ BT thực hiện (mẫu số)',NULL
	FROM tmp_data_dgq t1;
	
	WITH tmp_vung_amount AS 
	(
		SELECT vung, SUM(amount) as amount
		FROM tmp_data_dgq
		GROUP BY vung
	)
	UPDATE tmp_result t1
		LEFT JOIN tmp_vung_amount t2 ON t1.vung = t2.vung
	SET t1.value = t2.amount
	WHERE t1.value IS NULL AND t1.ma_chi_tieu = 'TD_TLBT_MAU';
	
	-- So HSBT da giai quyet theo level:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung,t1.department_code,t1.employee_code,
	    CASE t1.`level`
	        WHEN 1 THEN 'GD_C1'
	        WHEN 2 THEN 'GD_C2'
	        WHEN 3 THEN 'GD_C3'
	        WHEN 4 THEN 'GD_C4'
	        WHEN 5 THEN 'GD_C5'
	        WHEN 6 THEN 'GD_C6'
	        WHEN 7 THEN 'GD_C7'
	        WHEN 8 THEN 'GD_C8'
	    END AS ma_chi_tieu,
	    CASE t1.`level`
	        WHEN 1 THEN 'Giám định C1'
	        WHEN 2 THEN 'Giám định C2'
	        WHEN 3 THEN 'Giám định C3'
	        WHEN 4 THEN 'Giám định C4'
	        WHEN 5 THEN 'Giám định C5'
	        WHEN 6 THEN 'Giám định C6'
	        WHEN 7 THEN 'Giám định C7'
	        WHEN 8 THEN 'Giám định C8'
	    END AS ten_chi_tieu,
	    COUNT(DISTINCT t1.claim_id) AS value
	FROM tmp_data_dgq t1
	WHERE t1.amount > 0
	  	AND t1.level BETWEEN 1 AND 8
	GROUP BY t1.vung,t1.department_code,t1.employee_code, t1.`level`;
	
	-- tong HSBT da giai quyet:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung,t1.department_code,t1.employee_code,
	    'GD_HSBT_dgq' AS ma_chi_tieu,
	    'Tổng HSBT đã giải quyết' ten_chi_tieu,
	    COUNT(DISTINCT t1.claim_id) AS value
	FROM tmp_data_dgq t1
	WHERE t1.amount > 0
	  	AND t1.level BETWEEN 1 AND 8
	GROUP BY t1.vung,t1.department_code,t1.employee_code;

	-- So HSBT check theo level: 
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung,t1.department_code,t1.tier1_checker_user,
	    CASE t1.`level`
	        WHEN 1 THEN 'TD_check_C1'
	        WHEN 2 THEN 'TD_check_C2'
	        WHEN 3 THEN 'TD_check_C3'
	        WHEN 4 THEN 'TD_check_C4'
	        WHEN 5 THEN 'TD_check_C5'
	        WHEN 6 THEN 'TD_check_C6'
	        WHEN 7 THEN 'TD_check_C7'
	        WHEN 8 THEN 'TD_check_C8'
	    END AS ma_chi_tieu,
	    CASE t1.`level`
	        WHEN 1 THEN 'Check C1'
	        WHEN 2 THEN 'Check C2'
	        WHEN 3 THEN 'Check C3'
	        WHEN 4 THEN 'Check C4'
	        WHEN 5 THEN 'Check C5'
	        WHEN 6 THEN 'Check C6'
	        WHEN 7 THEN 'Check C7'
	        WHEN 8 THEN 'Check C8'
	    END AS ten_chi_tieu,
	    COUNT(DISTINCT t1.claim_id) AS value
	FROM tmp_ngay_check_ngay_validate t1
	WHERE t1.amount > 0
	  	AND DATE(t1.ngay_check) = v_ngay
	  	AND t1.level BETWEEN 1 AND 8
	GROUP BY t1.vung, t1.department_code,t1.tier1_checker_user, t1.`level`;
	
	-- So HSBT check theo level: 
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung,t1.department_code,t1.validate_user,
	    CASE t1.`level`
	        WHEN 1 THEN 'TD_validate_C1'
	        WHEN 2 THEN 'TD_validate_C2'
	        WHEN 3 THEN 'TD_validate_C3'
	        WHEN 4 THEN 'TD_validate_C4'
	        WHEN 5 THEN 'TD_validate_C5'
	        WHEN 6 THEN 'TD_validate_C6'
	        WHEN 7 THEN 'TD_validate_C7'
	        WHEN 8 THEN 'TD_validate_C8'
	    END AS ma_chi_tieu,
	    CASE t1.`level`
	        WHEN 1 THEN 'Validate C1'
	        WHEN 2 THEN 'Validate C2'
	        WHEN 3 THEN 'Validate C3'
	        WHEN 4 THEN 'Validate C4'
	        WHEN 5 THEN 'Validate C5'
	        WHEN 6 THEN 'Validate C6'
	        WHEN 7 THEN 'Validate C7'
	        WHEN 8 THEN 'Validate C8'
	    END AS ten_chi_tieu,
	    COUNT(DISTINCT t1.claim_id) AS value
	FROM tmp_ngay_check_ngay_validate t1
	WHERE t1.amount > 0
	  	AND DATE(t1.ngay_validate) = v_ngay
	  	AND t1.level BETWEEN 1 AND 8
	GROUP BY t1.vung, t1.department_code,t1.validate_user, t1.`level`;

-- 	GD: nang suat muc tieu
-- 	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
-- 	WITH tmp_employee AS 
-- 	(
-- 	    SELECT DISTINCT employee_code
-- 	    FROM tmp_raw_data
-- 	)
-- 	SELECT t2.vung, t2.department_code, t1.employee_code,'GD_NSMT' AS ma_chi_tieu,'Năng suất mục tiêu' AS ten_chi_tieu,
-- 	    IF(t2.vi_tri = 'TP', t3.head_employee_goals,t3.employee_goals) AS value
-- 	FROM tmp_employee t1
-- 		LEFT JOIN data_analysis.dim_sos_employee t2 ON t1.employee_code = t2.employee_code
-- 		LEFT JOIN data_analysis.dim_sos_gd_productivity_goals t3 ON t2.department_code = t3.department_code;
-- 	
-- 	TD: nang suat muc tieu
-- 	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
-- 	SELECT t2.vung, t2.department_code, t1.employee_code,'TD_NSMT' AS ma_chi_tieu,'Năng suất mục tiêu' AS ten_chi_tieu,
-- 	    t1.employee_goals AS value
-- 	FROM data_analysis.dim_sos_td_productivity_goals t1
-- 		LEFT JOIN data_analysis.dim_sos_employee t2 ON t1.employee_code  = t2.employee_code;
	
	-- GD: hsbt ton dau ky 
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t2.`user`,'GD_HSBT_ton_dau_ky' AS ma_chi_tieu,'HSBT tồn đầu kỳ' AS ten_chi_tieu,
	    COUNT(DISTINCT t1.claim_id) AS value
	FROM tmp_claim_raw t1
		LEFT JOIN analysis.factless_claim t2 ON t1.claim_id = t2.claim_id AND t2.valid  = 1
	WHERE t1.system_date < DATE(v_ngay) AND DATE(t1.claim_approved_date) >= v_ngay 
	GROUP BY t1.vung, t1.department_code, t2.`user` ;

	-- GD: hsbt phat sinh trong ky
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t2.`user`,'GD_HSBT_phat_sinh' AS ma_chi_tieu,'HSBT phát sinh' AS ten_chi_tieu,
	    COUNT(DISTINCT t1.claim_id) AS value
	FROM tmp_claim_raw t1
		LEFT JOIN analysis.factless_claim t2 ON t1.claim_id = t2.claim_id AND t2.valid  = 1
	WHERE t1.system_date = DATE(v_ngay)
	GROUP BY t1.vung, t1.department_code, t2.`user` ;
	
	-- GD: hsbt ton lau ngay
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.employee_code,
	    CASE 
	        WHEN t1.level = 1 THEN 'GD_ton_lau_lv1'
	        WHEN t1.level = 2 THEN 'GD_ton_lau_lv2'
	        WHEN t1.level = 3 THEN 'GD_ton_lau_lv3'
	        WHEN t1.level = 4 THEN 'GD_ton_lau_lv4'
	        WHEN t1.level = 5 THEN 'GD_ton_lau_lv5'
	        WHEN t1.level = 6 THEN 'GD_ton_lau_lv6'
	        WHEN t1.level = 7 THEN 'GD_ton_lau_lv7'
	        WHEN t1.level = 8 THEN 'GD_ton_lau_lv8'
	    END AS ma_chi_tieu,
	    CASE 
	        WHEN t1.level = 1 THEN 'Level 1 tồn lâu'
	        WHEN t1.level = 2 THEN 'Level 2 tồn lâu'
	        WHEN t1.level = 3 THEN 'Level 3 tồn lâu'
	        WHEN t1.level = 4 THEN 'Level 4 tồn lâu'
	        WHEN t1.level = 5 THEN 'Level 5 tồn lâu'
	        WHEN t1.level = 6 THEN 'Level 6 tồn lâu'
	        WHEN t1.level = 7 THEN 'Level 7 tồn lâu'
	        WHEN t1.level = 8 THEN 'Level 8 tồn lâu'
	    END AS ten_chi_tieu,
	    COUNT(DISTINCT 
	        CASE 
	            WHEN t1.`level` IN (1,2) AND DATEDIFF(v_ngay,DATE(t1.system_date)) > 45 THEN t1.claim_id
	            WHEN t1.`level` IN (3,4) AND DATEDIFF(v_ngay,DATE(t1.system_date)) > 60 THEN t1.claim_id
	            WHEN t1.`level` IN (5,6,7,8) AND DATEDIFF(v_ngay,DATE(t1.system_date)) > 90 THEN t1.claim_id
	        END
	    ) AS value
	FROM tmp_data_ton_lau t1 
	WHERE t1.amount > 0
	    -- AND LEFT(t1.business_type_code,2) = 'XO'
	    AND t1.level BETWEEN 1 AND 8	
	    -- AND t1.claim_approved_date > CONCAT(v_ngay,' 23:59:59') 
	GROUP BY t1.vung, t1.department_code, t1.employee_code, t1.`level`;

	-- GD: CPBQ năm nay tử số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.employee_code,
		'GD_CPBQ_TU', 'Chi phí bình quân (tử số)', SUM(t1.amount) 
	FROM tmp_data_dgq t1
		LEFT JOIN analysis.motor_fact_motor_claim_approved_price t2 ON t1.claim_id = t2.claim_id
	WHERE t1.amount > 0
		AND t1.amount < 10000000
		AND t1.business_type_code = 'XO.4.1.1'
		AND t2.deductible_quantity > 0
	GROUP BY t1.vung, t1.department_code, t1.employee_code;
	
	-- GD: CPBQ năm nay mẫu số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.employee_code,
		'GD_CPBQ_MAU', 'Chi phí bình quân (mẫu số)', SUM(t2.deductible_quantity) 
	FROM tmp_data_dgq t1
		LEFT JOIN analysis.motor_fact_motor_claim_approved_price t2 ON t1.claim_id = t2.claim_id
	WHERE t1.amount > 0
		AND t1.amount < 10000000
		AND t1.business_type_code = 'XO.4.1.1'
		AND t2.deductible_quantity > 0
	GROUP BY t1.vung, t1.department_code, t1.employee_code;
	
	-- GD: CPBQ năm trước tử số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.employee_code,
		'GD_CPBQ_TU_NT', 'Chi phí bình quân (tử số)', SUM(t1.amount) 
	FROM tmp_data_dgq_NT t1
		LEFT JOIN analysis.motor_fact_motor_claim_approved_price t2 ON t1.claim_id = t2.claim_id
	WHERE t1.amount > 0
		AND t1.amount < 10000000
		AND t1.business_type_code = 'XO.4.1.1'
		AND t2.deductible_quantity > 0
	GROUP BY t1.vung, t1.department_code, t1.employee_code;
	
	-- GD: CPBQ năm trước mẫu số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.employee_code,
		'GD_CPBQ_MAU_NT', 'Chi phí bình quân (mẫu số)', SUM(t2.deductible_quantity) 
	FROM tmp_data_dgq_NT t1 
		LEFT JOIN analysis.motor_fact_motor_claim_approved_price t2 ON t1.claim_id = t2.claim_id
	WHERE t1.amount > 0
		AND t1.amount < 10000000
		AND t1.business_type_code = 'XO.4.1.1'
		AND t2.deductible_quantity > 0
	GROUP BY t1.vung, t1.department_code, t1.employee_code;
	
	IF DAY(v_ngay) IN (21) THEN
		-- GD: ước hsbt ton dau ky 
		INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
		SELECT t1.vung, t1.department_code, t1.employee_code,'GD_GT_HSBT_ton_dau_ky' AS ma_chi_tieu,'Ước HSBT tồn đầu kỳ' AS ten_chi_tieu,
			SUM(t1.uoc_dau_ky) AS value
		FROM tmp_estimate_amount_result t1
		GROUP BY t1.vung, t1.department_code, t1.employee_code;
	END IF;

	IF DAY(v_ngay) IN (20) THEN
		-- 	GD: uoc cuoi ky:
		INSERT INTO tmp_result(vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
		SELECT t1.vung, t1.department_code, t1.employee_code,
		    'GD_GT_HSBT_ton_cuoi_ky' AS ma_chi_tieu,'Ước HSBT tồn cuối kỳ' AS ten_chi_tieu,
		    SUM(t1.uoc_cuoi_ky) AS value
		FROM tmp_estimate_amount_result t1
		GROUP BY t1.vung, t1.department_code, t1.employee_code;
	END IF;

	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t3.`user` ,
		'GD_mo_sau_2_ngay', 'Số HSBT mở sau 2 ngày nhập call',
		COUNT(DISTINCT t1.claim_id) as value 
	FROM tmp_claim_raw t1
		LEFT JOIN analysis.factless_claim_call_center t2 ON t1.call_center_id = t2.call_id AND t1.manage_unit_code = t2.unit_code AND t2.valid = 1	
		LEFT JOIN analysis.factless_claim t3 ON t1.claim_id = t3.claim_id AND t3.valid  = 1
	WHERE t1.system_date = DATE(v_ngay)
	  AND (
	        DATEDIFF(DATE(t1.system_date), DATE(t2.system_date))
	        - (WEEK(DATE(t1.system_date), 1) - WEEK(DATE(t2.system_date), 1)) * 2
	        - CASE 
	            WHEN DAYOFWEEK(DATE(t1.system_date)) = 7 THEN 1   -- nếu ngày kết thúc là Thứ 7
	            WHEN DAYOFWEEK(DATE(t2.system_date)) = 1 THEN 1 -- nếu ngày bắt đầu là Chủ nhật
	            ELSE 0
	          END
	      ) > 2
	GROUP BY t1.vung, t1.department_code, t3.`user`;
	
	-- TD: CPBQ năm nay tử số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.employee_code,
		'TD_CPBQ_TU', 'Chi phí bình quân (tử số)', SUM(t1.amount) 
	FROM tmp_data_dgq t1
		LEFT JOIN analysis.motor_fact_motor_claim_approved_price t2 ON t1.claim_id = t2.claim_id
	WHERE t1.amount > 0
		AND t1.amount < 20000000
		AND t1.business_type_code = 'XO.4.1.1'
		AND t2.deductible_quantity > 0
	GROUP BY t1.vung, t1.department_code, t1.employee_code;
	
	-- TD: CPBQ năm nay mẫu số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.employee_code,
		'TD_CPBQ_MAU', 'Chi phí bình quân (mẫu số)', SUM(t2.deductible_quantity) 
	FROM tmp_data_dgq t1
		LEFT JOIN analysis.motor_fact_motor_claim_approved_price t2 ON t1.claim_id = t2.claim_id
	WHERE t1.amount > 0
		AND t1.amount < 20000000
		AND t1.business_type_code = 'XO.4.1.1'
		AND t2.deductible_quantity > 0
	GROUP BY t1.vung, t1.department_code, t1.employee_code;
	
	-- TD: CPBQ năm trước tử số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.employee_code,
		'TD_CPBQ_TU_NT', 'Chi phí bình quân (tử số)', SUM(t1.amount) 
	FROM tmp_data_dgq_NT t1
		LEFT JOIN analysis.motor_fact_motor_claim_approved_price t2 ON t1.claim_id = t2.claim_id
	WHERE t1.amount > 0
		AND t1.amount < 20000000
		AND t1.business_type_code = 'XO.4.1.1'
		AND t2.deductible_quantity > 0
	GROUP BY t1.vung, t1.department_code, t1.employee_code;
	
	-- TD: CPBQ năm trước mẫu số:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.employee_code,
		'TD_CPBQ_MAU_NT', 'Chi phí bình quân (mẫu số)', SUM(t2.deductible_quantity) 
	FROM tmp_data_dgq_NT t1 
		LEFT JOIN analysis.motor_fact_motor_claim_approved_price t2 ON t1.claim_id = t2.claim_id
	WHERE t1.amount > 0
		AND t1.amount < 20000000
		AND t1.business_type_code = 'XO.4.1.1'
		AND t2.deductible_quantity > 0
	GROUP BY t1.vung, t1.department_code, t1.employee_code;

	-- TD: make chuyen check:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.received_user,
		'TD_check_nhan_moi', 'Check nhận mới trong kỳ', COUNT(DISTINCT t1.claim_id) 
	FROM tmp_approval_log t1
	WHERE DATE(t1.created_date) = v_ngay
		AND t1.`action` = 'M_CHUYEN_C'
	GROUP BY t1.vung, t1.department_code, t1.received_user;

	-- TD: check hoan thanh, chuyen cho validate:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.sent_user,
		'TD_check_hoan_thanh', 'Check hoàn thành trong kỳ', COUNT(DISTINCT t1.claim_id) 
	FROM tmp_approval_log t1
	WHERE DATE(t1.created_date) = v_ngay
		AND t1.`action` = 'C_CHUYEN_V'
	GROUP BY t1.vung, t1.department_code, t1.sent_user;

	-- TD: check hoan thanh dung han:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.sent_user,
		'TD_check_dung_han', 'Check hoàn thành trong kỳ', COUNT(DISTINCT t1.claim_id) 
	FROM tmp_approval_log t1
	WHERE DATE(t1.created_date) = v_ngay
		AND t1.`action` = 'C_CHUYEN_V'
		AND IFNULL(t1.working_seconds_diff,0) <= han_check
	GROUP BY t1.vung, t1.department_code, t1.sent_user;

	-- TD: check chuyen validate:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.received_user,
		'TD_validate_nhan_moi', 'Validate nhận mới trong kỳ', COUNT(DISTINCT t1.claim_id) 
	FROM tmp_approval_log t1
	WHERE DATE(t1.created_date) = v_ngay
		AND t1.`action` = 'C_CHUYEN_V'
	GROUP BY t1.vung, t1.department_code, t1.received_user;
	
	-- TD: validate duyet:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.sent_user,
		'TD_validate_hoan_thanh', 'Validate hoàn thành trong kỳ', COUNT(DISTINCT t1.claim_id) 
	FROM tmp_approval_log t1
	WHERE DATE(t1.created_date) = v_ngay
		AND t1.`action` IN ('V_DUYET','V_DUYET_DG_BL')
	GROUP BY t1.vung, t1.department_code, t1.sent_user;
	
	-- TD: validate hoan thanh dung han:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t1.sent_user,
		'TD_validate_dung_han', 'Check hoàn thành trong kỳ', COUNT(DISTINCT t1.claim_id) 
	FROM tmp_approval_log t1
	WHERE DATE(t1.created_date) = v_ngay
		AND t1.`action` IN ('V_DUYET','V_DUYET_DG_BL')
		AND IFNULL(t1.working_seconds_diff,0) <= han_validate
	GROUP BY t1.vung, t1.department_code, t1.sent_user;

-- 	TD: check chua hoan thanh:
	INSERT INTO tmp_result ( vung, department_code, employee_code,ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung,t1.department_code, t1.tier1_checker_user,
	    'TD_Check_chua_duyet',
	    'Số HSBT chưa check xong',
	    COUNT(DISTINCT t1.claim_id)
	FROM tmp_ngay_check_ngay_validate t1
	WHERE t1.ngay_nhan_check <= CONCAT(v_ngay, ' 17:00:00') AND (t1.ngay_huy_nhan_check > CONCAT(v_ngay, ' 17:00:00') OR t1.ngay_huy_nhan_check IS NULL)
		AND (t1.ngay_check > CONCAT(v_ngay, ' 17:00:00') OR t1.ngay_check IS NULL)
	    AND t1.tier1_checker_user IS NOT NULL
	GROUP BY t1.vung, t1.department_code, t1.tier1_checker_user;

	-- TD: validate chua hoan thanh:
	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung,t1.department_code,t1.validate_user,
	    'TD_validate_chua_duyet',
	    'Số HSBT chưa validate xong',
	    COUNT(DISTINCT t1.claim_id)
	FROM tmp_ngay_check_ngay_validate t1
	WHERE t1.ngay_check <= CONCAT(v_ngay, ' 17:00:00') AND (t1.ngay_huy_nhan_validate > CONCAT(v_ngay, ' 17:00:00') OR t1.ngay_huy_nhan_validate IS NULL)
		AND (t1.ngay_validate > CONCAT(v_ngay, ' 17:00:00') OR t1.ngay_validate IS NULL)
	GROUP BY t1.vung,t1.department_code,t1.validate_user;

	-- tinh cac chi tieu cua truong phong:
	CREATE TEMPORARY TABLE tmp_sum_tp AS 
	SELECT t2.department_code,MAX(t3.ct_code) AS vung,  
		t1.ma_chi_tieu , MAX(t1.ten_chi_tieu) as ten_chi_tieu,  SUM(t1.value) as value
	FROM tmp_result t1
		LEFT JOIN 
		(
			SELECT employee_code, department_code 
			FROM analysis.core_dim_employee 
			WHERE valid = 1 AND unit_code = '000'
		) t2 ON t1.employee_code = t2.employee_code -- group lai theo phong ban ma gdv do truc thuoc, ko theo phong thu ly ho so
		LEFT JOIN tmp_vung t3 ON t2.department_code = t3.department_code
	WHERE t1.ma_chi_tieu IN ('GD_HSBT_ton_dau_ky','GD_HSBT_phat_sinh','GD_ton_lau_lv1','GD_ton_lau_lv2'
		,'GD_ton_lau_lv3','GD_ton_lau_lv4','GD_ton_lau_lv5','GD_ton_lau_lv6','GD_ton_lau_lv7','GD_ton_lau_lv8','GD_GT_HSBT_ton_dau_ky'
		,'GD_GT_HSBT_ton_cuoi_ky','GD_mo_sau_2_ngay', 'GD_HSBT_dgq')
	GROUP BY t2.department_code, t1.ma_chi_tieu;
	
	DELETE t1
	FROM tmp_result t1
		INNER JOIN data_analysis.dim_sos_employee t2 ON t1.employee_code = t2.employee_code AND t2.vi_tri  = 'TP'
	WHERE t1.ma_chi_tieu IN ('GD_HSBT_ton_dau_ky','GD_HSBT_phat_sinh','GD_ton_lau_lv1','GD_ton_lau_lv2'
		,'GD_ton_lau_lv3','GD_ton_lau_lv4','GD_ton_lau_lv5','GD_ton_lau_lv6','GD_ton_lau_lv7','GD_ton_lau_lv8','GD_GT_HSBT_ton_dau_ky'
		,'GD_GT_HSBT_ton_cuoi_ky','GD_mo_sau_2_ngay', 'GD_HSBT_dgq');
		
	INSERT INTO tmp_result(vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, 
		t2.employee_code, t1.ma_chi_tieu , t1.ten_chi_tieu, t1.value
	FROM tmp_sum_tp t1
		LEFT JOIN data_analysis.dim_sos_employee t2 ON t1.department_code = t2.department_code AND t2.vi_tri  = 'TP';
	
	-- chi tieu vizualize:
	INSERT INTO tmp_result(vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t2.`user` ,'GD_so_ngay_mo_hs_sau_call' AS ma_chi_tieu,
		'Tổng số ngày mở HSBT sau khi nhận call' AS ten_chi_tieu,
	    SUM( 
		 	IF(DATEDIFF(DATE(t1.system_date), DATE(t3.system_date))
	        - (WEEK(DATE(t1.system_date), 1) - WEEK(DATE(t3.system_date), 1)) * 2
	        - CASE 
	            WHEN DAYOFWEEK(DATE(t1.system_date)) = 7 THEN 1   -- nếu ngày kết thúc là Thứ 7
	            WHEN DAYOFWEEK(DATE(t3.system_date)) = 1 THEN 1 -- nếu ngày bắt đầu là Chủ nhật
	            ELSE 0
	          END >0, 
	          DATEDIFF(DATE(t1.system_date), DATE(t3.system_date))
	        - (WEEK(DATE(t1.system_date), 1) - WEEK(DATE(t3.system_date), 1)) * 2
	        - CASE 
	            WHEN DAYOFWEEK(DATE(t1.system_date)) = 7 THEN 1   -- nếu ngày kết thúc là Thứ 7
	            WHEN DAYOFWEEK(DATE(t3.system_date)) = 1 THEN 1 -- nếu ngày bắt đầu là Chủ nhật
	            ELSE 0
	          END,0)
		) AS value
	FROM tmp_claim_raw t1
		LEFT JOIN analysis.factless_claim t2 ON t1.claim_id = t2.claim_id AND t2.valid  = 1
		LEFT JOIN analysis.factless_claim_call_center t3 ON t1.call_center_id = t3.call_id AND t1.manage_unit_code = t3.unit_code AND t3.valid = 1	
	WHERE t1.system_date = DATE(v_ngay)
	GROUP BY t1.vung, t1.department_code, t2.`user`;
	
	INSERT INTO tmp_result(vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t2.`user` ,'GD_HSBT_phat_sinh_vizualize' AS ma_chi_tieu,
		'HSBT phát sinh trong kỳ' AS ten_chi_tieu,
	    COUNT(DISTINCT t1.claim_id) AS value
	FROM tmp_claim_raw t1
		LEFT JOIN analysis.factless_claim t2 ON t1.claim_id = t2.claim_id AND t2.valid  = 1
	WHERE t1.system_date = DATE(v_ngay)
	GROUP BY t1.vung, t1.department_code, t2.`user`;

	INSERT INTO tmp_result (vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value)
	SELECT t1.vung, t1.department_code, t2.`user` ,'GD_HSBT_ton_dau_ky_vizualize' AS ma_chi_tieu,
		'HSBT tồn đầu kỳ' AS ten_chi_tieu,
	    COUNT(DISTINCT t1.claim_id) AS value
	FROM tmp_claim_raw t1
		LEFT JOIN analysis.factless_claim t2 ON t1.claim_id = t2.claim_id AND t2.valid  = 1
	WHERE t1.system_date < DATE(v_ngay)
		AND DATE(t1.claim_approved_date) >= v_ngay 
	GROUP BY t1.vung, t1.department_code, t2.`user` ;

	DELETE dst
	FROM data_analysis.kpi_report_dataset dst
	WHERE dst.ngay = v_ngay;

    INSERT INTO data_analysis.kpi_report_dataset(ngay ,thang_bao_cao, vung, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value,
    	created_date)
	SELECT v_ngay, report_month,
		CASE WHEN vung = 'MN' THEN 'Miền Nam'
			WHEN vung = 'MB' THEN 'Miền Bắc' END, department_code, employee_code, ma_chi_tieu, ten_chi_tieu, value, CURRENT_TIMESTAMP()
	FROM tmp_result
	WHERE value <> 0 AND vung IS NOT NULL;

	-- SELECT 'Procedure kpi_report_dataset executed successfully' AS message;
END
