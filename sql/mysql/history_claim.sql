/*
Problem:
I need to create a table to track the change history of claim records. 
Each record contains a lot of related information, and if even a single piece of information changes, it should be inserted into the dataset table with the date of the change recorded.

Solution:
I wrote a procedure that runs daily to check whether any claim has changed compared to its most recent entry in the dataset table. 
If there is a change, a new record is inserted; if not, nothing is inserted.
*/

CREATE DEFINER=`hoangdm`@`162.32.16.53` PROCEDURE `analysis_db`.`claim_history`(in running_date DATE)
BEGIN
	-- HoangDM: this procedure runs daily with a date value of T-1 (yesterday).
	DECLARE v_ngay DATE;
	SET v_ngay = IFNULL(running_date, CURDATE() - INTERVAL 1 DAY);

	-- HoangDM: I created temporary tables to retrieve the data needed throughout the procedure:
	DROP TEMPORARY TABLE IF EXISTS tmp_claim_by_clause;
	CREATE TEMPORARY TABLE tmp_claim_by_clause AS
	SELECT t1.unit_code,t1.claim_id,t1.object_id,
    	GROUP_CONCAT(t1.business_type_code ORDER BY t1.business_type_code ASC SEPARATOR ',') AS business_type_code
  	FROM analysis.claim_by_clause t1 
		INNER JOIN analysis.factless_claim t2 ON t1.claim_id  = t2.claim_id  AND t1.unit_code  = t2.unit_code
 	WHERE t2.claim_id  BETWEEN '20251201000000' AND  '20260228999999' 
	  AND v_ngay BETWEEN t2.eff_date AND t2.end_date 
  	GROUP BY t1.unit_code,t1.claim_id,t1.object_id;

	-- HoangDM: This is temporary table containing  the results:
	DROP TEMPORARY TABLE IF EXISTS tmp_raw_data;
	CREATE TEMPORARY TABLE tmp_raw_data (
	    fk_date DATE,
	    claim_id BIGINT,
	    claim_no VARCHAR(150),
	    category VARCHAR(50),
	    policy_id BIGINT,
	    policy_number VARCHAR(100),
	    certificate_id BIGINT,
	    certificate VARCHAR(100),
	    business_type_code TEXT,
	    region VARCHAR(100),
	    unit_code VARCHAR(20),
	    manage_unit_code VARCHAR(20),
	    manage_unit_name VARCHAR(100),
	    department_code VARCHAR(20),
		department_name VARCHAR(100),
		vip_customer_type VARCHAR(50),	
		general_agency VARCHAR(50),
		tier_2_agency VARCHAR(50),
		tier_3_agency VARCHAR(50),
		general_sale_channel VARCHAR(50),
		tier_2_sale_channel VARCHAR(50),
		tier_3_sale_channel VARCHAR(50),
		custid VARCHAR(20),
		fee_payment_date DATETIME,	
	    incident_date DATE,
	    notifield_date DATETIME,
	    event_code VARCHAR(50),
	    system_date DATE,
	    assessment_date DATETIME,
	  -- garage_entry_date DATE,
	  -- garage_exit_date DATE,			
	  -- pending_cause TEXT,
	  -- cause_code VARCHAR(50),
	    garage_code VARCHAR(50),
	    garage_name VARCHAR(255),
	    motor_make VARCHAR(100),
	    motor_model_code VARCHAR(100),
	    motor_model VARCHAR(255),
		production_year INT,		
	    license_plate VARCHAR(50),
	    chassis_number VARCHAR(100),
	    motor_owner_name TEXT,
	  -- pay_garage_flag VARCHAR(10),
	    number_of_deduction INT,
	    claim_detail_status INT,
	    claim_detail_status_name VARCHAR(50),   
		price_make_date DATETIME,
		price_check_date DATETIME,
		price_validate_date DATETIME,
		price_approval_make_date DATETIME,
		price_approval_check_date DATETIME,
		price_approval_validate_date DATETIME,
		payment_require_make_date DATETIME,
		payment_require_check_date DATETIME,
		payment_require_validate_date DATETIME,
	    claim_approved_date DATE,
		-- payment_date DATE,
		maker_user VARCHAR(15),
	    tier1_checker_user VARCHAR(100),
	    tier2_checker_user VARCHAR(100),
	    validate_user VARCHAR(100),
	    compensation_amount_lcy TEXT,
	  -- price_approval_amount DECIMAL(25,3),
	  -- payment_amount DECIMAL(25,3)
	    custid_tong TEXT,
	    certificate_effective_date DATETIME,
	    call_center_id BIGINT,
	    call_date DATETIME,
	    guarantee_date DATETIME,
	    garage_payment_amt DECIMAL(30,2),
	    garage_payment_date DATETIME,
	    customer_payment_amt DECIMAL(30,2),
	    customer_payment_date DATETIME,
	    total_repair_amt DECIMAL(30,2),
	    lifting_amt DECIMAL(30,2),
	    compensation_amt DECIMAL(30,2), -- tien giam tru
	    risk_shared_amt DECIMAL(30,2),
	    covered_repair_amt DECIMAL(30,2),
	    covered_lifting_amt DECIMAL(30,2),
	    customer_advance_amt TEXT,
	    customer_advance_date TEXT,
	    customer_advance_recovery_amt DECIMAL(30,2),
	    garage_advance_amt TEXT, 
	    garage_advance_date TEXT,
	    garage_advance_recovery_amt DECIMAL(30,2),
	    total_recovery_amt TEXT,
	    row_hash CHAR(32)
	);

	-- HoangDM: First, i insert serveral fields i can calculate now into tmp_raw_data:
	INSERT INTO tmp_raw_data(fk_date, claim_id, claim_no, category,policy_id, policy_number, certificate_id,certificate,
	  	business_type_code, region,
		unit_code, manage_unit_code, manage_unit_name,
		department_code,department_name, general_agency,tier_2_agency, tier_3_agency,
		general_sale_channel,tier_2_sale_channel, tier_3_sale_channel,
		custid, incident_date, notifield_date,event_code, system_date,
		assessment_date, garage_code, garage_name, motor_make,
		motor_model_code, motor_model,production_year, license_plate, chassis_number, motor_owner_name, 
		number_of_deduction, claim_detail_status,claim_detail_status_name,
		claim_approved_date, maker_user, tier1_checker_user, tier2_checker_user, validate_user,
		custid_tong,call_center_id,guarantee_date, total_repair_amt, lifting_amt, compensation_amt, risk_shared_amt, covered_repair_amt,
		customer_advance_recovery_amt, garage_advance_recovery_amt
	)
	SELECT v_ngay AS fk_date,
		t1.claim_id, t1.claim_no, t1.category, t3.policy_id , t3.policy_number, 
		t2.object_id as certificate_id, t4.certificate, t2.business_type_code,
		CASE WHEN t1.department_code IN (000, 090, 'MB.TD', 'MB.TD', 002, 0030, 050, 051, 093, 005, 006, 013, 014, 015, 019, 021, 022, 023, 030, 034, 039, 040, 043, 044, 050, 051,
					'090.P1', '090.P2', '090.P3', '090.P4', '090.P5', '090.P6', '090.TTL', 093) THEN 'Miền Bắc'
			  WHEN t1.department_code IN ('MN', 'MN.TD', 091, 'MN', 'MN.TD', 0010, 004, 007, 008, 009, 010, 011, 012, 017, 018, 033, 035, 046, 049, 052, 053, 
					'091.P1', '091.P2', '091.P3', '091.P5', '091.P7', '091.TTL', '091.P9', 042) THEN 'Miền Nam'
			  ELSE '' END AS region,
		t1.unit_code, t1.manage_unit_code,t6.unit_name,
		t1.department_code, t7.department_name , 
		IFNULL(IF(t3.sale_channel_type = 'D',t5.custid_tong,'KHAC'),'KHAC') as general_agency,
		IFNULL(IF(t3.sale_channel_type = 'D',t10.general_agency_channel,'KHAC'),'KHAC') as tier_2_agency,
		IFNULL(IF(t3.sale_channel_type = 'D', t3.sale_code, 'KHAC'),'KHAC') as tier_3_agency,
		IFNULL(t5.custid_tong,'KHAC') as general_sale_channel,
		IFNULL(CASE WHEN IF(t3.sale_channel_type = 'D',t5.custid_tong,'KHAC') = 'VNPOST' AND t3.custid_kt IS NULL THEN IF(t3.sale_channel_type = 'D',t10.general_agency_channel,'KHAC') 
			  ELSE t5.company_code END,'KHONG_KENH_KT') AS tier_2_sale_channel,
		IFNULL(CASE WHEN IF(t3.sale_channel_type = 'D',t5.custid_tong,'KHAC') = 'VNPOST' AND t3.custid_kt IS NULL THEN IF(t3.sale_channel_type = 'D', t3.sale_code, 'KHAC') 
			ELSE t3.custid_kt END,'KHONG_KENH_KT') AS tier_3_sale_channel,
		t3.custid, DATE(t1.incident_date) as incident_date, 
		DATE(t1.notifield_date) as notifield_date, t1.sk_code, DATE(t1.system_date) as system_date,
		DATE(t1.assessment_date) as assessment_date, 
		t1.garage_code ,t1.garage_name , t1.motor_make , t1.motor_model_code , t1.motor_model,
		t4.production_year, t1.license_plate , t1.chassis_number , t1.motor_owner_name,
		IFNULL(t8.deductible_quantity,0) as number_of_deduction,
		t1.claim_detail_status,
		CASE claim_detail_status
		    WHEN 1 THEN 'Đã hủy'
		    WHEN 3 THEN 'Giám định chi tiết'
		    WHEN 4 THEN 'Trình công nhận giá'
		    WHEN 5 THEN 'Check công nhận giá'
		    WHEN 6 THEN 'Chờ duyệt Công nhận giá'
		    WHEN 7 THEN 'Đã duyệt công nhận giá'
		    WHEN 8 THEN 'Trình duyệt giá'
		    WHEN 9 THEN 'Check duyệt giá'
		    WHEN 10 THEN 'Chờ duyệt duyệt giá'
		    WHEN 11 THEN 'Đã duyệt duyệt giá'
		    WHEN 12 THEN 'Trình bảo lãnh'
		    WHEN 13 THEN 'Đã bảo lãnh'
		    WHEN 14 THEN 'Trình đề nghị thanh toán'
		    WHEN 15 THEN 'Check đề nghị thanh toán'
		    WHEN 16 THEN 'Chờ duyệt đề nghị thanh toán'
		    WHEN 17 THEN 'Đã duyệt đề nghị thanh toán'
		    WHEN 18 THEN 'Đã chuyển thông tin ĐNTT'
		    WHEN 19 THEN 'Chờ duyệt ngoại lệ'
		    WHEN 20 THEN 'Chờ thanh toán'
		    WHEN 21 THEN 'Chờ xử lý lỗi chi tiền qua bank'
		    WHEN 22 THEN 'Đã thanh toán'
		    WHEN 23 THEN 'Từ chối thanh toán'
		    ELSE '' END AS claim_detail_status_name,
		DATE(t1.claim_approved_date) as claim_approved_date, 
		IFNULL(t9.`user`,t9.claims_adjuster) as maker_user,t1.tier1_checker_user, t1.tier2_checker_user , t1.validate_user, t5.custid_tong,
		t1.call_center_id,t1.guarantee_date,
		t8.replacement_amt + t8.special_replacement_amt + t8.repair_amt + t8.repainting_amt + t8.labor_amt as total_repair_amt,
		t8.lifting_amt as lifting_amt, t8.compensation_amt as compensation_amt, t8.risk_shared_amt as risk_shared_amt,
		null as covered_repair_amt,
		t11.advance_payment_amount_lcy,t12.amount_lcy
	FROM analysis.factless_claim t1
	  	INNER JOIN tmp_claim_by_clause t2 ON t1.unit_code = t2.unit_code AND t1.claim_id = t2.claim_id
		LEFT JOIN analysis.factless_policy t3 ON t1.manage_unit_code = t3.unit_code AND t1.motor_policy_id = t3.policy_id 
			AND v_ngay BETWEEN t3.eff_date AND t3.end_date -- AND t3.product_type IN ('XE', 'XEL')
		LEFT JOIN analysis.car_insurance_certificate t4 ON t1.manage_unit_code = t4.unit_code AND t1.motor_policy_id = t4.motor_policy_id AND t2.object_id = t4.object_id 
			AND v_ngay BETWEEN t4.eff_date AND t4.end_date
		LEFT JOIN analysis.dim_customer t5 ON t5.custid = t3.custid_kt AND t5.unit_code = t1.unit_code
			AND v_ngay BETWEEN t5.eff_date AND t5.end_date
		LEFT JOIN analysis.dim_unit t6 ON t1.manage_unit_code = t6.unit_code 
			AND v_ngay BETWEEN t6.eff_date AND t6.end_date
		LEFT JOIN analysis.dim_department t7 ON t1.unit_code = t7.unit_code AND t1.department_code = t7.department_code
			AND v_ngay BETWEEN t7.eff_date AND t7.end_date
		LEFT JOIN analysis.history_approved t8 ON t1.unit_code = t8.unit_code AND t1.claim_id = t8.claim_id 
			AND t8.price_approve_amt  <> 0 -- AND v_ngay >= t8.proposed_date
		LEFT JOIN analysis.car_factless_claim t9 ON t1.unit_code = t9.unit_code AND t1.claim_id = t9.claim_id 
			AND v_ngay BETWEEN t9.eff_date AND t9.end_date AND t9.product_type IN ('XE', 'XEL')
		LEFT JOIN analysis.dim_agency t10 ON t3.unit_code = t10.unit_code and CAST(t3.sale_code AS BINARY) = CAST(t10.agency_id AS BINARY)
			AND v_ngay BETWEEN t10.eff_date AND t10.end_date
		LEFT JOIN analysis.claim_advance_payment t11 ON t1.claim_id = t11.claim_id and t1.unit_code = t11.unit_code AND t11.payment_type='T' 
			AND v_ngay BETWEEN t11.eff_date AND t11.end_date
		LEFT JOIN analysis.garage_debt t12 ON t1.claim_id = t12.claim_id and t1.unit_code = t12.unit_code AND t12.document_type='C' 
			AND v_ngay BETWEEN t12.eff_date AND t12.end_date
	WHERE t1.claim_id  BETWEEN '20251201000000' AND  '20260228999999'
		AND v_ngay BETWEEN t1.eff_date AND t1.end_date;

	--HoangDM: I create indexs for temp table tmp_raw_data:
	CREATE INDEX idx_tmp_raw_data ON tmp_raw_data(manage_unit_code, call_center_id);
	CREATE INDEX idx_tmp_raw_data2 ON tmp_raw_data(claim_id,unit_code);

	--HoangDM: Continue calculating the remaining data fields.
	DROP TEMPORARY TABLE IF EXISTS tmp_estimate_amount;
	CREATE TEMPORARY TABLE tmp_estimate_amount
	(
	    claim_id BIGINT,
	    unit_code VARCHAR(15),
	    business_type_code TEXT,
	    claim_approved_date DATETIME,
	    change_date DATE,
	    estimated_amount TEXT
	);

	INSERT INTO tmp_estimate_amount(claim_id,unit_code, business_type_code,claim_approved_date, change_date, estimated_amount)
	SELECT t1.claim_id, t1.unit_code, MAX(t2.business_type_code) AS business_type_code,DATE(MAX(t2.claim_approved_date)) AS claim_approved_date,
	    DATE(t1.request_approve_premium_date) AS change_date,
	    GROUP_CONCAT(CAST(t1.request_amount_lcy AS UNSIGNED) ORDER BY t1.business_type_code ASC SEPARATOR ',') AS estimated_amount
	FROM analysis.estimate_amount  t1
		INNER JOIN tmp_raw_data t2 ON t1.claim_id  = t2.claim_id
	WHERE DATE(request_approve_premium_date) <= v_ngay
		AND t1.request_approve_premium_date IS NOT NULL
	GROUP BY t1.claim_id, t1.unit_code,t1.request_approve_premium_date;
		
	INSERT INTO tmp_estimate_amount(claim_id, unit_code, business_type_code, claim_approved_date,change_date, estimated_amount)
	SELECT t1.claim_id,t1.unit_code,MAX(t2.business_type_code) AS business_type_code, 
		DATE(MAX(t2.claim_approved_date)) AS claim_approved_date, MAX(t2.claim_approved_date),
		GROUP_CONCAT(CAST(t1.compensation_amount AS UNSIGNED) ORDER BY t1.business_type_code ASC SEPARATOR ',') AS estimated_amount
	FROM analysis.claim_by_clause  t1 
		INNER JOIN tmp_raw_data t2 ON t1.claim_id = t2.claim_id
	WHERE t2.claim_approved_date <> '3000-01-01'
	GROUP BY t1.claim_id,t1.unit_code;

	DROP TEMPORARY TABLE IF EXISTS tmp_compensation_amount_lcy;
	CREATE TEMPORARY TABLE tmp_compensation_amount_lcy AS
	SELECT claim_id,unit_code,business_type_code,change_date, estimated_amount, 
		RANK() OVER(PARTITION BY claim_id ORDER BY change_date DESC) AS rn
	FROM tmp_estimate_amount;
		
	CREATE INDEX idx_tmp_compensation_amount_lcy ON tmp_compensation_amount_lcy(unit_code, claim_id);

	UPDATE tmp_raw_data t1
		LEFT JOIN tmp_compensation_amount_lcy t2 ON t1.unit_code = t2.unit_code AND t1.claim_id = t2.claim_id 
			AND t1.business_type_code = t2.business_type_code AND t2.rn = 1
	SET t1.compensation_amount_lcy = IFNULL(t2.estimated_amount,0)
	WHERE t1.compensation_amount_lcy IS NULL;
	/*---------------------------------------------------------------------------------------------------------------*/
	DROP TEMPORARY TABLE IF EXISTS tmp_log;
	CREATE TEMPORARY TABLE tmp_log AS
	SELECT
	    t1.claim_id,
	    MAX(CASE WHEN t1.action = 'M_CHUYEN_C' AND t1.mcv_price_approve_type = 'C' THEN t1.created_date END) AS price_make_date,
	    MAX(CASE WHEN t1.action = 'C_CHUYEN_V' AND t1.mcv_price_approve_type = 'C' THEN t1.created_date END) AS price_check_date,
	    MAX(CASE WHEN t1.action = 'V_DUYET' AND t1.mcv_price_approve_type = 'C' THEN t1.created_date END) AS price_validate_date,
	    
	    MAX(CASE WHEN t1.action = 'M_CHUYEN_C' AND t1.mcv_price_approve_type = 'D' THEN t1.created_date END) AS price_approval_make_date,
	    MAX(CASE WHEN t1.action = 'C_CHUYEN_V' AND t1.mcv_price_approve_type = 'D' THEN t1.created_date END) AS price_approval_check_date,
	    MAX(CASE WHEN t1.action IN ('V_DUYET', 'V_DUYET_DG_BL') AND t1.mcv_price_approve_type = 'D' THEN t1.created_date END) AS price_approval_validate_date,
	    
	    MAX(CASE WHEN t1.action = 'M_TRINH_C_DNTT' THEN t1.created_date END) AS payment_require_make_date,
	   	MAX(CASE WHEN t1.action = 'C_CHUYEN_V_DNTT' THEN t1.created_date END) AS payment_require_check_date,
	    MAX(CASE WHEN t1.action = 'DUYET_QDINH' THEN t1.created_date END) AS payment_require_validate_date
	FROM analysis.appoved_log t1
		INNER JOIN tmp_raw_data t2 ON t1.claim_id = t2.claim_id
	WHERE t1.created_date <= v_ngay
	GROUP BY t1.claim_id;

	CREATE INDEX idx_tmp_log ON tmp_log(claim_id);
	/*---------------------------------------------------------------------------------------------------------------*/
	UPDATE tmp_raw_data t1
		LEFT JOIN tmp_log t2 ON t1.claim_id = t2.claim_id 
	SET t1.price_make_date = t2.price_make_date, 
		t1.price_check_date = t2.price_check_date,
		t1.price_validate_date = t2.price_validate_date,
		t1.price_approval_make_date = t2.price_approval_make_date ,
		t1.price_approval_check_date = t2.price_approval_check_date,
		t1.price_approval_validate_date = t2.price_approval_validate_date,
		t1.payment_require_make_date = t2.payment_require_make_date,
		t1.payment_require_check_date = t2.payment_require_check_date,
		t1.payment_require_validate_date = t2.payment_require_validate_date 
	WHERE t1.price_make_date IS NULL;

	/*---------------------------------------------------------------------------------------------------------------*/
	DROP TEMPORARY TABLE IF EXISTS tmp_dim_vip_customer;
	CREATE TEMPORARY TABLE tmp_dim_vip_customer as 
	SELECT custid , unit_code , vip_customer_type_name FROM analysis.dim_vip_customer 
	WHERE v_ngay BETWEEN eff_date AND end_date;

	CREATE INDEX idx_tmp_dim_vip_customer ON tmp_dim_vip_customer(unit_code(15),custid(50));

	UPDATE tmp_raw_data t1
		LEFT JOIN tmp_dim_vip_customer t2 ON t1.unit_code = t2.unit_code AND t1.custid = t2.custid 
	SET t1.vip_customer_type = 
	IFNULL(t2.vip_customer_type_name, CASE 
								    WHEN t1.vip_customer_type = -41 THEN 'VIP 1 KT'
								    WHEN t1.vip_customer_type = -40 THEN 'VIP 1 KKT'
								    WHEN t1.vip_customer_type = -31 THEN 'VIP 2 KT'
								    WHEN t1.vip_customer_type = -30 THEN 'VIP 2 KKT'
								    WHEN t1.vip_customer_type = -21 THEN 'VIP 1 KT P.KHL'
								    WHEN t1.vip_customer_type = -20 THEN 'VIP 1 KKT P.KHL'
								    WHEN t1.vip_customer_type = -11 THEN 'VIP 2 KT P.KHL'
								    WHEN t1.vip_customer_type = -10 THEN 'VIP 2 KKT P.KHL'
								    WHEN t1.vip_customer_type = 0 THEN 'KH thông thường'
								    WHEN t1.vip_customer_type = 10 THEN 'KH xấu'
								    WHEN t1.vip_customer_type = 20 THEN 'Đ.lý/Môi giới VIP - Các đối tượng/KH đ.lý/Môi giới này khai thác được coi là VIP'
								    WHEN t1.vip_customer_type = 30 THEN 'Kênh khai thác VIP - Các đối tượng/KH kênh này khai thác được coi là VIP'
								    ELSE 'Không xác định' END)
	WHERE t1.vip_customer_type IS NULL;

	/*---------------------------------------------------------------------------------------------------------------*/
	DROP TEMPORARY TABLE IF EXISTS tmp_garage_debt_T;
	CREATE TEMPORARY TABLE tmp_garage_debt_T AS 
	SELECT claim_id , unit_code,
		GROUP_CONCAT(CAST(amount_lcy AS UNSIGNED)  ORDER BY system_date ASC SEPARATOR ';') as garage_advance_amt,
		GROUP_CONCAT(date(system_date)  ORDER BY system_date ASC SEPARATOR ';') as garage_advance_date
	FROM analysis.garage_debt cfgd 
	WHERE document_type  = 'T'
	GROUP BY claim_id, unit_code ;

	UPDATE tmp_raw_data t1
		LEFT JOIN tmp_garage_debt_T t2 ON t1.claim_id = t2.claim_id 
	SET t1.garage_advance_amt = t2.garage_advance_amt,
		t1.garage_advance_date = t2.garage_advance_date
	WHERE 1=1;

	/*---------------------------------------------------------------------------------------------------------------*/
	SET SESSION group_concat_max_len = 1000000;
	DROP TEMPORARY TABLE IF EXISTS tmp_claim_advance_payment_C;
	CREATE TEMPORARY TABLE tmp_claim_advance_payment_C AS 
	SELECT claim_id , unit_code,
		GROUP_CONCAT(CAST(advance_payment_amount_lcy AS UNSIGNED)  ORDER BY system_date ASC SEPARATOR ';') as customer_advance_amt,
		GROUP_CONCAT(date(system_date)  ORDER BY system_date ASC SEPARATOR ';') as customer_advance_date
	FROM  analysis.claim_advance_payment cfgd 
	WHERE payment_type  = 'C'
	GROUP BY claim_id, unit_code ;
	
	CREATE INDEX idx_tmp_claim_advance_payment_C ON tmp_claim_advance_payment_C(unit_code(10), claim_id);

	UPDATE tmp_raw_data t1
		LEFT JOIN tmp_claim_advance_payment_C t2 on t1.unit_code = t2.unit_code AND t1.claim_id = t2.claim_id 
	SET t1.customer_advance_amt = t2.customer_advance_amt,
		t1.customer_advance_date = t2.customer_advance_date
	WHERE 1=1;
	/*---------------------------------------------------------------------------------------------------------------*/
	UPDATE tmp_raw_data t1
		LEFT JOIN analysis.factless_claim_call_center t2 ON t1.manage_unit_code = t2.unit_code  AND t1.call_center_id = t2.call_id 
			AND t2.valid  = 1
	SET t1.call_date = t2.created_date 
	WHERE 1=1;
	/*---------------------------------------------------------------------------------------------------------------*/
	DROP TEMPORARY TABLE IF EXISTS tmp_guarantee_date;
	CREATE TEMPORARY TABLE tmp_guarantee_date AS 
	SELECT t1.claim_id, t1.unit_code , MAX(t1.created_date) AS created_date 
	FROM analysis.appoved_log t1
		INNER JOIN tmp_raw_data t2 ON t2.manage_unit_code = t1.unit_code AND t1.claim_id = t2.claim_id
	WHERE t1.`action` IN ('V_DUYET_DG_BL','V_DUYET_BL')
	GROUP BY t1.claim_id, t1.unit_code;
	
	UPDATE tmp_raw_data t1
		LEFT JOIN tmp_guarantee_date t2 ON t1.manage_unit_code = t2.unit_code AND t1.claim_id = t2.claim_id
	SET t1.guarantee_date = t2.created_date
	WHERE t1.category in ('XE','XEL');
	/*---------------------------------------------------------------------------------------------------------------*/
	DROP TEMPORARY TABLE IF EXISTS tmp_garage_claim_payment;
	CREATE TEMPORARY TABLE tmp_garage_claim_payment AS 
	SELECT  t1.claim_id,t1.unit_code, SUM(t1.payment_amount_lcy) AS payment_amount_lcy, MAX(DATE(t1.system_date)) AS system_date
	FROM analysis.garage_claim_payment t1 
	  INNER JOIN tmp_raw_data t2 ON t2.unit_code = t1.unit_code AND t1.claim_id = t2.claim_id
	WHERE v_ngay BETWEEN t1.eff_date AND t1.end_date
	GROUP BY t1.claim_id,t1.unit_code;
	
	CREATE INDEX idx_tmp_garage_claim_payment ON tmp_garage_claim_payment(unit_code(10), claim_id);

	UPDATE tmp_raw_data t1
		LEFT JOIN tmp_garage_claim_payment t2 ON t2.unit_code = t1.unit_code AND t1.claim_id = t2.claim_id
	SET t1.garage_payment_amt = t2.payment_amount_lcy, t1.garage_payment_date = t2.system_date
	WHERE 1=1;
	/*---------------------------------------------------------------------------------------------------------------*/
	DROP TEMPORARY TABLE IF EXISTS tmp_claim_payment;
	CREATE TEMPORARY TABLE tmp_claim_payment AS 
	SELECT  t1.claim_id,t1.unit_code, SUM(t1.payment_amount_lcy) AS payment_amount_lcy, MAX(t1.created_date) AS created_date
	FROM analysis.claim_payment t1 
		INNER JOIN tmp_raw_data t2 ON t1.claim_id = t2.claim_id AND t2.unit_code = t1.unit_code
	WHERE v_ngay BETWEEN t1.eff_date AND t1.end_date
	GROUP BY t1.claim_id,t1.unit_code;
	
	UPDATE tmp_raw_data t1
		LEFT JOIN tmp_claim_payment t2 ON t1.claim_id = t2.claim_id AND t2.unit_code = t1.unit_code
	SET t1.customer_payment_amt = t2.payment_amount_lcy, t1.customer_payment_date = t2.created_date
	WHERE 1=1;
	/*---------------------------------------------------------------------------------------------------------------*/
	UPDATE tmp_raw_data t1
		LEFT JOIN analysis.car_insurance_certificate t2 ON t1.certificate_id = t2.object_id AND t2.unit_code = t1.manage_unit_code
			AND t1.policy_id = t2.motor_policy_id
			AND v_ngay BETWEEN t2.eff_date AND t2.end_date 
	SET t1.certificate_effective_date = TIMESTAMP(t2.certificate_effective_date, IFNULL(REPLACE(REPLACE(t2.certificate_effective_time,'H',':'),'h',':'),0))
	WHERE t1.category IN ('XE','XEL');

	UPDATE tmp_raw_data t1
		LEFT JOIN analysis.motor_factless_motorbike_insurance_certificate t2 ON t1.certificate_id = t2.object_id AND t2.unit_code = t1.manage_unit_code
			AND t1.policy_id = t2.motor_policy_id
			AND v_ngay BETWEEN t2.eff_date AND t2.end_date 
	SET t1.certificate_effective_date = TIMESTAMP(t2.certificate_effective_date, IFNULL(REPLACE(REPLACE(t2.certificate_effective_time,'H',':'),'h',':'),0))
	WHERE t1.category ='2B';

	UPDATE tmp_raw_data t1
		LEFT JOIN analysis.motor_factless_motorbike_insurance_single_certificate t2 ON t1.certificate_id = t2.certificate_id AND t2.unit_code = t1.manage_unit_code
			AND v_ngay BETWEEN t2.eff_date AND t2.end_date 
	SET t1.certificate_effective_date = TIMESTAMP(t2.certificate_effective_date, IFNULL(REPLACE(REPLACE(t2.certificate_effective_time,'H',':'),'h',':'),0))
	WHERE t1.category ='2BL';
	/*---------------------------------------------------------------------------------------------------------------*/
	UPDATE tmp_raw_data t1
		LEFT JOIN analysis.dim_customer t2 ON t2.custid = t1.garage_code AND t1.unit_code = t2.unit_code
			AND v_ngay BETWEEN t2.eff_date AND t2.end_date 
	SET t1.custid_tong = t2.custid_tong
	WHERE 1=1;

	-- HoangDM: I deleted the data at date T-1 in the final physical results table (data_analysis.history_claim) so that i could overwrite it.
	DELETE dst
	FROM data_analysis.history_claim dst
	WHERE dst.fk_date = v_ngay;
	/*---------------------------------------------------------------------------------------------------------------*/
	-- HoangDM: Because I only insert records that have changed since the most recent date into the final results table, I'm using the row_hash field for comparison.
	-- I use an MD5 hash function that aggregates all data fields and compares it to the most recent date to see if it differs, instead of having to compare all data fields.
	UPDATE tmp_raw_data  
	SET row_hash = 
	MD5(CONCAT_WS('|',
        claim_id,claim_no,policy_id, policy_number,certificate_id,certificate,business_type_code,region,
   		unit_code, manage_unit_code, manage_unit_name, department_code, department_name, vip_customer_type, 
   		general_agency, tier_2_agency, tier_3_agency,general_sale_channel,tier_2_sale_channel,tier_3_sale_channel,
    	custid,fee_payment_date, incident_date, notifield_date, event_code, system_date, assessment_date,
	    garage_code, garage_name, motor_make, motor_model_code, motor_model, production_year,
	    license_plate, chassis_number, motor_owner_name, number_of_deduction, claim_detail_status,
	    claim_detail_status_name, price_make_date, price_check_date, price_validate_date, price_approval_make_date,
	    price_approval_check_date, price_approval_validate_date, payment_require_make_date, payment_require_check_date,
	    payment_require_validate_date, claim_approved_date, maker_user,
	    tier1_checker_user, tier2_checker_user, validate_user, compensation_amount_lcy,
	    custid_tong, certificate_effective_date, call_date, guarantee_date,
	    garage_payment_amt, garage_payment_date, customer_payment_amt, customer_payment_date,
	    total_repair_amt,lifting_amt, compensation_amt, risk_shared_amt, covered_repair_amt,
	    covered_lifting_amt, customer_advance_amt, customer_advance_date,
	    customer_advance_recovery_amt, garage_advance_amt, garage_advance_date,
	    garage_advance_recovery_amt, total_recovery_amt
    ))
    WHERE row_hash IS NULL;
   
	INSERT INTO data_analysis.history_claim
	(
    	fk_date, claim_id,claim_no,policy_id, policy_number,certificate_id,certificate,business_type_code,region,
   		unit_code, manage_unit_code, manage_unit_name, department_code, department_name, vip_customer_type, 
   		general_agency, tier_2_agency, tier_3_agency,general_sale_channel,tier_2_sale_channel,tier_3_sale_channel,
    	custid,fee_payment_date, incident_date, notifield_date, event_code, system_date, assessment_date,
	    garage_code, garage_name, motor_make, motor_model_code, motor_model, production_year,
	    license_plate, chassis_number, motor_owner_name, number_of_deduction, claim_detail_status,
	    claim_detail_status_name, price_make_date, price_check_date, price_validate_date, price_approval_make_date,
	    price_approval_check_date, price_approval_validate_date, payment_require_make_date, payment_require_check_date,
	    payment_require_validate_date, claim_approved_date, maker_user,
	    tier1_checker_user, tier2_checker_user, validate_user, compensation_amount_lcy,
	    custid_tong, certificate_effective_date, call_date, guarantee_date,
	    garage_payment_amt, garage_payment_date, customer_payment_amt, customer_payment_date,
	    total_repair_amt, lifting_amt, compensation_amt, risk_shared_amt, covered_repair_amt,
	    covered_lifting_amt, customer_advance_amt, customer_advance_date,
	    customer_advance_recovery_amt, garage_advance_amt, garage_advance_date,
	    garage_advance_recovery_amt, total_recovery_amt, row_hash, create_date
	)
 	SELECT r.fk_date, r.claim_id, r.claim_no, r.policy_id, r.policy_number, r.certificate_id, r.certificate, r.business_type_code, r.region,
		r.unit_code, r.manage_unit_code, r.manage_unit_name, r.department_code, r.department_name, r.vip_customer_type,
		r.general_agency, r.tier_2_agency, r.tier_3_agency, r.general_sale_channel, r.tier_2_sale_channel, r.tier_3_sale_channel,
		r.custid, r.fee_payment_date, r.incident_date, r.notifield_date, r.event_code, r.system_date, r.assessment_date,
		r.garage_code, r.garage_name, r.motor_make, r.motor_model_code, r.motor_model, r.production_year,
		r.license_plate, r.chassis_number, r.motor_owner_name, r.number_of_deduction, r.claim_detail_status,
		r.claim_detail_status_name, r.price_make_date, r.price_check_date, r.price_validate_date, r.price_approval_make_date,
		r.price_approval_check_date, r.price_approval_validate_date, r.payment_require_make_date, r.payment_require_check_date,
		r.payment_require_validate_date, r.claim_approved_date, r.maker_user,
		r.tier1_checker_user, r.tier2_checker_user, r.validate_user, r.compensation_amount_lcy,
		r.custid_tong, r.certificate_effective_date, r.call_date, r.guarantee_date,
		r.garage_payment_amt, r.garage_payment_date, r.customer_payment_amt, r.customer_payment_date,
		r.total_repair_amt, r.lifting_amt, r.compensation_amt, r.risk_shared_amt, r.covered_repair_amt,
		r.covered_lifting_amt, r.customer_advance_amt, r.customer_advance_date,
		r.customer_advance_recovery_amt, r.garage_advance_amt, r.garage_advance_date,
		r.garage_advance_recovery_amt, r.total_recovery_amt, r.row_hash, CURRENT_TIMESTAMP() 
	FROM tmp_raw_data r
	WHERE NOT EXISTS 
	(
		SELECT 1 FROM history_claim d
		WHERE d.claim_id = r.claim_id 
			AND r.row_hash = d.row_hash
			AND d.fk_date < v_ngay
	);
END
