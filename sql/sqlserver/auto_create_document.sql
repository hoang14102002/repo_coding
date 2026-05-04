--=================================================================
-- Author: HoangDM
-- Created: 2/8/24
-- Description: Procedure for generating assembly transfer-out slips based on quality inspection results
/*After the Quality Control (QC) department completes the product quality assessment, the warehouse department is responsible for 
converting non-conforming finished goods into alternative finished products. 
This procedure enables the automatic creation of corresponding inbound and outbound inventory slips, helping to resolve 
inventory discrepancies during the process of assembling new finished goods.*/

--=================================================================
ALTER PROCEDURE dbo.usp_Create_B7
	@_IdList XML = NULL,
	@_LangId INT = 0,
	@_BranchCode VARCHAR(3) = 'A01',
	@_DocCode VARCHAR(2) = 'LR',
	@_nUserId INT = 0,
	@_FiscalYear  AS VARCHAR(4) =  '2024',
	@_Month AS VARCHAR(2) = '02',
	@_CommandKey VARCHAR(24) = 'Created_LR'
AS
BEGIN
    SET NOCOUNT ON;

	DECLARE @hdoc INT, @_Stt_Tmp NVARCHAR(48), @_DocNo_Tmp NVARCHAR(48), @_RowId_DocProcess NVARCHAR(24), @_DocProcessId INT
		, @_DocStatus TINYINT, @_CustomerId INT, @_ExchangeRate NUMERIC(18,2), @_TransCode CodeType
		, @_SttListXML NVARCHAR(MAX), @_Year INT, @_DocNoFormat VARCHAR(8), @_StrExec NVARCHAR(MAX), @_DocNo1 VARCHAR(20), @params  NVARCHAR(MAX)
		, @_Description NVARCHAR(256), @_DocDate DATE
		, @_NeedRePost TINYINT --Cần dọn dẹp lại trong trường hợp Delete phiếu cũ
		, @_MinDate_RePost DATE, @_MaxDate_Repost DATE --Khoảng ngày Repost

			SET @_NeedRePost = 0
			SET @_MinDate_RePost = '2024-01-01'
			SET @_MaxDate_Repost = '2024-01-01'

	EXEC sp_xml_preparedocument @hdoc OUTPUT, @_IdList
	DROP TABLE IF EXISTS #Data
	SELECT *
	INTO #Data 
	FROM OPENXML (@hdoc, '/NewDataSet/Select' , 1) 
	WITH (
			IsTick TINYINT,RowId VARCHAR(16), ItemId_Delivery INT, WarehouseId_Delivery INT, Quantity9 QuantityType
			, ItemId_Receipt INT, WarehouseId_Receipt INT, ReceiptQuantity QuantityType, LotCode CodeType
			, LotCodeT5 CodeType
			, RollCode VARCHAR(24), CustomerId INT, TransCode CodeType, DocNoFormat VARCHAR(8)
			, Description NVARCHAR(256), DocDate DATE
		)

	DELETE FROM #Data WHERE IsTick = 0

	IF EXISTS (SELECT * FROM dbo.B30AccDocItem t1
					INNER JOIN #Data t2 ON t2.RowId = t1.RowId_B7
				WHERE t1.BranchCode = @_BranchCode
			  )
	BEGIN
		SELECT @_MinDate_RePost = MIN(t1.DocDate), @_MaxDate_Repost = MAX(t1.DocDate)
		FROM dbo.B30AccDocItem t1
			INNER JOIN #Data t2 ON t2.RowId = t1.RowId_B7
		WHERE t1.BranchCode = @_BranchCode

	    SET  @_NeedRePost = 1
	END

	UPDATE #Data -- HoangDM: Same warehouse
	SET WarehouseId_Delivery = WarehouseId_Receipt
	WHERE ISNULL(WarehouseId_Delivery,0) <> WarehouseId_Receipt

	SELECT TOP 1 @_CustomerId = CustomerId, @_DocNoFormat = DocNoFormat 
		, @_Description = Description, @_DocDate = DocDate
	FROM #Data

	SELECT TOP 1 @_TransCode = TransCode FROM #Data WHERE ISNULL(TransCode,'') <> ''

	SET @_DocNoFormat = 'LR' + @_DocNoFormat
	
	-- HoangDM: Delete old document:
	DELETE t1 -- HoangDM: Delete import detail
	FROM dbo.B30AccDocItem1 t1
		LEFT OUTER JOIN dbo.B30AccDocItem t2 ON t1.Stt = t2.Stt
		INNER JOIN #Data t3 ON t2.RowId_B7 = t3.RowId		
	WHERE t2.BranchCode = @_BranchCode

	DELETE t1 -- HoangDM: Delete export detail
	FROM dbo.B30AccDocItem3 t1
		LEFT OUTER JOIN dbo.B30AccDocItem t2 ON t1.Stt = t2.Stt
		INNER JOIN #Data t3 ON t2.RowId_B7 = t3.RowId		
	WHERE t2.BranchCode = @_BranchCode

	DELETE t1 -- HoangDM: Delete phys table
	FROM dbo.B30AccDocInventoryPhys t1
		LEFT OUTER JOIN dbo.B30AccDocItem t2 ON t1.Stt = t2.Stt
		INNER JOIN #Data t3 ON t2.RowId_B7 = t3.RowId		
	WHERE t2.BranchCode = @_BranchCode
	
	DELETE t1 -- HoangDM: Delete header
	FROM dbo.B30AccDocItem t1
		INNER JOIN #Data t2 ON t2.RowId = t1.RowId_B7
	WHERE t1.BranchCode = @_BranchCode

	-- HoangDM: get DocStatus by user
	SELECT @_DocStatus = dbo.ufn_B00DmCt_DocStatusDefault(@_DocCode, @_nUserId, @_BranchCode )
	
	-- HoangDM: create temporary table:
	DROP TABLE IF EXISTS #Ct
	DROP TABLE IF EXISTS #Ct0
	DROP TABLE IF EXISTS #Ct1
	DROP TABLE IF EXISTS #Phys_Ct --Bảng con của đầu phiếu 
	DROP TABLE IF EXISTS #Phys_Ct0 --Bảng con của chi tiết

	-- HoangDM: header:
	SELECT CAST(NULL AS VARCHAR(16)) AS Stt, CAST(NULL AS VARCHAR(48)) AS DocNo
		, RowId AS RowId_B7, ItemId_Receipt, WarehouseId_Receipt, ReceiptQuantity
		, CAST(NULL AS VARCHAR(16)) AS RowId_DocProcess,CAST(NULL AS VARCHAR(16)) AS DocProcessId
		, ROW_NUMBER() OVER(ORDER BY ItemId_Delivery) AS Rankk
	INTO #Ct FROM #Data

	-- HoangDM: export detail:
	SELECT CAST(NULL AS VARCHAR(16)) AS Stt, CAST(NULL AS VARCHAR(16)) AS RowId, RowId AS RowId_B7
		, ItemId_Delivery, WarehouseId_Delivery, LotCodeT5, Quantity9
	INTO #Ct0 FROM #Data

	-- HoangDM: import detail:
	SELECT CAST(NULL AS VARCHAR(16)) AS Stt, CAST(NULL AS VARCHAR(16)) AS RowId, RowId AS RowId_B7
		, ItemId_Receipt, WarehouseId_Receipt, ReceiptQuantity, LotCode
	INTO #Ct1 FROM #Data

	-- HoangDM: Phys header:
	SELECT CAST(NULL AS VARCHAR(16)) AS Stt, CAST(NULL AS VARCHAR(16)) AS RowId, CAST(NULL AS VARCHAR(16)) AS RowId_SourceDoc
		, 1 AS DocGroup
		, ItemId_Receipt, WarehouseId_Receipt, LotCode, RollCode
		, ReceiptQuantity, RowId AS RowId_B7
	INTO #Phys_Ct FROM #Data

	-- HoangDM: Phys detail:
	SELECT 2 AS DocGroup,CAST(NULL AS VARCHAR(16)) AS Stt, CAST(NULL AS VARCHAR(16)) AS RowId
		, CAST(NULL AS VARCHAR(16)) AS RowId_SourceDoc
		, ItemId_Delivery, WarehouseId_Delivery, LotCodeT5, Quantity9, RollCode, RowId AS RowId_B7
	INTO #Phys_Ct0 FROM #Data
	

	-- HoangDM: Get DocNo increasing auto:
	SET @_StrExec = '
	SELECT TOP 1 @_DocNo1OUT = DocNo FROM dbo.B30AccDocItem WHERE DocNo LIKE '''+@_DocNoFormat+'%'' ORDER By DocNo DESC
	'
	SET @params = N'@_DocNo1OUT VARCHAR(20) OUTPUT';
	EXECUTE sp_executesql @_StrExec
		, @params 
		, @_DocNo1OUT = @_DocNo1 OUTPUT

	IF ISNULL(@_DocNo1, '') = ''
	BEGIN
	   UPDATE #Ct 
	   SET DocNo = @_DocNoFormat + '/' +FORMAT(CAST(@_Month AS INT),'D2') + '/' +RIGHT(CAST(@_FiscalYear AS INT),2) + '-' + FORMAT(Rankk, 'D6')
	END
	ELSE 
	BEGIN
	    UPDATE #Ct 
		SET	DocNo = @_DocNoFormat + '/' +FORMAT(CAST(@_Month AS INT),'D2') + '/' +RIGHT(CAST(@_FiscalYear AS INT),2) + '-' 
			+ FORMAT(Rankk + CAST(RIGHT(@_DocNo1, 6) AS INT), 'D6')
	END

	-- HoangDM: Update Stt and Rowid-----------------------------------------------------------------------------------------
	EXECUTE dbo.usp_sys_CreateSttBySeq
		@_SeqName = 'AccDoc_Seq',
		@_BranchCode = @_BranchCode,
		@_Ext = '',
		@_OutputType = 2,				-- 0: Output | 1: Select dữ liệu | 2: Cập nhật cho bảng temp
		@_OutputValue = '',				-- Giá trị output
		@_OutputTableName = '#Ct',	-- Update thẳng vào bảng temp
		@_OutputColName = 'Stt'
			
	EXECUTE dbo.usp_sys_CreateSttBySeq
		@_SeqName = 'AccDocDetail_Seq',
		@_BranchCode = @_BranchCode,
		@_Ext = @_DocCode,
		@_OutputType = 2,				-- 0: Output | 1: Select dữ liệu | 2: Cập nhật cho bảng temp
		@_OutputValue = '',				-- Giá trị output
		@_OutputTableName = '#Ct0',	-- Update thẳng vào bảng temp
		@_OutputColName = 'RowId'	

	EXECUTE dbo.usp_sys_CreateSttBySeq
		@_SeqName = 'AccDocDetail_Seq',
		@_BranchCode = @_BranchCode,
		@_Ext = @_DocCode,
		@_OutputType = 2,				-- 0: Output | 1: Select dữ liệu | 2: Cập nhật cho bảng temp
		@_OutputValue = '',				-- Giá trị output
		@_OutputTableName = '#Ct1',	-- Update thẳng vào bảng temp
		@_OutputColName = 'RowId'	

	EXECUTE dbo.usp_sys_CreateSttBySeq
		@_SeqName = 'AccDocDetail_Seq',
		@_BranchCode = @_BranchCode,
		@_Ext = '',
		@_OutputType = 2,				-- 0: Output | 1: Select dữ liệu | 2: Cập nhật cho bảng temp
		@_OutputValue = '',				-- Giá trị output
		@_OutputTableName = '#Phys_Ct',	-- Update thẳng vào bảng temp
		@_OutputColName = 'RowId'	

	EXECUTE dbo.usp_sys_CreateSttBySeq
		@_SeqName = 'AccDocDetail_Seq',
		@_BranchCode = @_BranchCode,
		@_Ext = '',
		@_OutputType = 2,				-- 0: Output | 1: Select dữ liệu | 2: Cập nhật cho bảng temp
		@_OutputValue = '',				-- Giá trị output
		@_OutputTableName = '#Phys_Ct0',	-- Update thẳng vào bảng temp
		@_OutputColName = 'RowId'	

	UPDATE t1 
	SET t1.Stt = t2.Stt
	FROM #Ct0 t1 LEFT OUTER JOIN #Ct t2 ON t2.RowId_B7 = t1.RowId_B7

	UPDATE t1 
	SET t1.Stt = t2.Stt
	FROM #Ct1 t1 LEFT OUTER JOIN #Ct t2 ON t2.RowId_B7 = t1.RowId_B7

	UPDATE t1 
	SET	t1.RowId_SourceDoc = t2.RowId, t1.Stt = t2.Stt
	FROM #Phys_Ct t1 LEFT OUTER JOIN #Ct1 t2 ON t1.RowId_B7 = t2.RowId_B7
	
	UPDATE t1 
	SET	t1.RowId_SourceDoc = t2.RowId, t1.Stt = t2.Stt
	FROM #Phys_Ct0 t1 LEFT OUTER JOIN #Ct0 t2 ON t1.RowId_B7 = t2.RowId_B7

	----------------------------------------------------------------------------------------------------
	SELECT TOP 1 @_Stt_Tmp = Stt FROM #Ct WHERE DocNo IS NULL
	EXEC dbo.usp_B20DocProcess_GetData 
			@_Id = -1										   
			,@_RowId_DocProcess = @_RowId_DocProcess OUTPUT
			,@_DocProcessId = @_DocProcessId OUTPUT
			,@_DocDate = @_DocDate
			,@_BranchCode = @_BranchCode
			,@_DocCode = @_DocCode
			,@_IsRunFromApp = NULL
			,@_IsSelectResult = NULL
			,@_LangId = 0

	UPDATE #Ct SET RowId_DocProcess = @_RowId_DocProcess, DocProcessId = @_DocProcessId

	DECLARE @_CurrencyCode VARCHAR(3) = ''
	SELECT @_CurrencyCode = VarValue FROM dbo.B00Config 
	WHERE VarKey = 'M_Ma_Tte0'

	INSERT INTO dbo.B30AccDocItem
	(
		BranchCode, Stt, DocCode, DocDate, DocNo, TransCode, DocGroup, Description
		, ReceiptWarehouseId, ReceiptItemId, ReceiptQuantity
		, CustomerId, DocStatus, RowId_DocProcess, DocProcessId, RowId_B7, CreatedBy, IsActive
		, TotalOriginalAmount0, TotalOriginalAmount3, TotalOriginalAmount
		, TotalAmount0, TotalAmount3, TotalAmount, CurrencyCode
	)
	SELECT @_BranchCode, t1.Stt, @_DocCode, @_DocDate, t1.DocNo, @_TransCode, 2, ISNULL(@_Description, '')
		--,N'Phiếu xuất lắp ráp tạo từ phiếu đánh giá block số: ' + t3.DocNo
		, t1.WarehouseId_Receipt, t1.ItemId_Receipt, t1.ReceiptQuantity, @_CustomerId, @_DocStatus, @_RowId_DocProcess, @_DocProcessId, t1.RowId_B7
		, @_nUserId, 1
		, 0, 0, 0
		, 0, 0, 0, @_CurrencyCode
	FROM #Ct t1
		LEFT OUTER JOIN dbo.B30QCBlockDetail t2 ON t1.RowId_B7 = t2.RowId 
		LEFT OUTER JOIN dbo.B30QCDoc t3 ON t2.QCDocId = t3.QCDocId 

	INSERT INTO dbo.B30AccDocItem1
	(
		BranchCode, Stt, RowId, DocCode, TransCode, DocGroup, CustomerId, WarehouseId, ItemId, Quantity9, Quantity, DocDate
		, DebitAccount, CreditAccount, CreatedBy, LotCode, IsActive
		, OriginalAmount, OriginalAmount9, OriginalAmount3, OriginalUnitCost
		, Amount, Amount9, Amount3, UnitCost
	)
	SELECT @_BranchCode, t1.Stt, t1.RowId, @_DocCode, @_TransCode, 1, @_CustomerId, t1.WarehouseId_Receipt AS WarehouseId, t1.ItemId_Receipt AS ItemId
		, t1.ReceiptQuantity AS Quantity9, t1.ReceiptQuantity AS Quantity, @_DocDate
		, t2.Account AS DebitAccount, t4.Account AS CreditAccount, @_nUserId, LotCode, 1
		, 0, 0, 0, 0
		, 0, 0, 0, 0
	FROM #Ct1 t1
		LEFT OUTER JOIN dbo.B20Warehouse t2 ON t1.WarehouseId_Receipt = t2.Id 
		LEFT OUTER JOIN #Ct0 t3 ON t1.RowId_B7 = t3.RowId_B7
		LEFT OUTER JOIN dbo.B20Warehouse t4 ON t3.WarehouseId_Delivery = t4.Id 

	INSERT INTO dbo.B30AccDocItem3
	(
		BranchCode, DocDate, Stt, RowId, BuiltinOrder, DocCode, TransCode, DocGroup, CustomerId, WarehouseId, ItemId, Unit
		, LotCode, Quantity9, Quantity, DebitAccount, CreditAccount, TotalRoll, BarrelQuantity, CreatedBy, IsActive
		, OriginalAmount, OriginalAmount9, OriginalUnitCost, OriginalAmount3
		, Amount, Amount9, UnitCost, Amount3
	)
	SELECT @_BranchCode, @_DocDate, t1.Stt, t1.RowId, 1, @_DocCode, @_TransCode, 2, @_CustomerId, t1.WarehouseId_Delivery AS WarehouseId
		, t1.ItemId_Delivery AS ItemId, t5.Unit, t1.LotCodeT5 
		, t1.Quantity9 AS Quantity9, t1.Quantity9 AS Quantity
		, t4.Account AS DebitAccount, t2.Account AS CreditAccount
		, t6.TotalRoll, CAST(IIF(t5.Packing IS NOT NULL,t1.Quantity9/t5.Packing, 0) AS NUMERIC(18,2)), @_nUserId, 1
		, 0, 0, 0, 0
		, 0, 0, 0, 0
	FROM #Ct0 t1
		LEFT OUTER JOIN dbo.B20Warehouse t2 ON t1.WarehouseId_Delivery = t2.Id 
		LEFT OUTER JOIN #Ct1 t3 ON t1.RowId_B7 = t3.RowId_B7
		LEFT OUTER JOIN dbo.B20Warehouse t4 ON t3.WarehouseId_Receipt = t4.Id 
		LEFT OUTER JOIN dbo.B20Item t5 ON t1.ItemId_Delivery = t5.Id 
		LEFT OUTER JOIN 
		(
			SELECT RowId_SourceDoc, COUNT(RowId) AS TotalRoll FROM  #Phys_Ct0
			GROUP BY RowId_SourceDoc
		) t6 ON t1.RowId = t6.RowId_SourceDoc

	-- HoangDM: Insert phys (B30AccDocInventoryPhys):
	INSERT INTO dbo.B30AccDocInventoryPhys
	(
		Stt, RowId, DocCode, RowId_SourceDoc, DocGroup, ItemId, WarehouseId, Quantity9, Quantity, Unit
		, RollCode, LotCode, DocDate, BranchCode, IsActive
		
	)
	SELECT t1.Stt, t1.RowId, @_DocCode, t1.RowId_SourceDoc, t1.DocGroup, t1.ItemId_Receipt AS ItemId
		, t1.WarehouseId_Receipt AS WarehouseId
		, t1.ReceiptQuantity AS Quantity9, t1.ReceiptQuantity AS Quantity, t2.Unit, t1.RollCode, ISNULL(t1.LotCode,'')
		, @_DocDate, @_BranchCode --GioiVM 19/08/24: Fix lại
		, 1
	FROM #Phys_Ct t1
		LEFT OUTER JOIN dbo.B20Item t2 ON t1.ItemId_Receipt = t2.Id 
	UNION ALL
	SELECT t1.Stt, t1.RowId, @_DocCode, t1.RowId_SourceDoc, t1.DocGroup, t1.ItemId_Delivery AS ItemId
		, t1.WarehouseId_Delivery AS WarehouseId, t1.Quantity9, t1.Quantity9 AS Quantity, t2.Unit
		, t1.RollCode, ISNULL(t1.LotCodeT5,'')
		, @_DocDate, @_BranchCode --GioiVM 19/08/24: Fix lại
		, 1
	FROM #Phys_Ct0 t1
		LEFT OUTER JOIN dbo.B20Item t2 ON t1.ItemId_Delivery = t2.Id 
	
	SELECT @_Year = YEAR(@_DocDate)
	---Post--------------------
	SET @_SttListXML = (SELECT Stt, @_DocDate AS DocDate, @_DocCode AS DocCode, @_DocStatus AS DocStatus FROM #Ct FOR XML RAW, ROOT('root'))

	EXECUTE usp_B30AccDoc_Post 
		@_BranchCode = @_BranchCode,
		@_Stt = NULL, @_DocDate = NULL, @_DocCode = NULL,
		@_DocStatus = NULL, 
		@_SttListXML = @_SttListXML,
		@_IsPostWhenSaveNewDoc = 1, 
		@_FiscalYear = @_Year

	IF @_NeedRePost = 1 
	BEGIN
		EXEC usp_B30AccDoc_RePost 
			@_StartDate = @_MinDate_RePost,
			@_EndDate = @_MaxDate_Repost,
			@_RePost = 1,
			@_ReBuildIndex=0,
			@_MoveToEventLog2 = 0,
			@_FiscalYear= @_FiscalYear,
			@_LangId= @_LangId,
			@_nUserId= @_nUserId,
			@_BranchCode =  @_BranchCode,
			@_RePostCurrentInventory = 1,
			@_RePostDuePayment = 0,
			@_CommandKey='RePost'
	END

	-- HoangDM: Table result
	SELECT t3.DocNo, t4.Code AS ItemCode_Delivery, t4.Name AS ItemName_Delivery 
		, t6.Code AS WarehouseCode_Delivery, t6.Name AS WarehouseName_Delivery 
		, t5.Code as ItemCode_Receipt, t5.Name as ItemName_Receipt
		, t7.Code as WarehouseCode_Receipt, t7.Name as WarehouseName_Receipt
		, t1.Quantity9, t1.ReceiptQuantity, N'Đã tạo phiếu xuất lắp ráp' AS Remark
	FROM #Data t1
		LEFT OUTER JOIN dbo.B30QCBlockDetail t2 ON t1.RowId = t2.RowId
		LEFT OUTER JOIN dbo.B30QCDoc t3 ON t2.QCDocId = t3.QCDocId
		LEFT OUTER JOIN dbo.B20Item t4 ON t1.ItemId_Delivery = t4.Id 
		LEFT OUTER JOIN dbo.B20Item t5 ON t1.ItemId_Receipt = t5.Id 
		LEFT OUTER JOIN dbo.B20Warehouse t6 ON t1.WarehouseId_Delivery = t6.Id 
		LEFT OUTER JOIN dbo.B20Warehouse t7 ON t1.WarehouseId_Receipt = t7.Id 

	-- HoangDM: Save log
	EXEC dbo.usp_sys_WriteLog_Command 
		@_Description = N'Tạo phiếu xuất lắp ráp từ đánh giá block',
		@_nUserId = @_nUserId,
		@_BranchCode = @_BranchCode,
		@_CommandKey = @_CommandKey


	DROP TABLE #Ct
	DROP TABLE #Phys_Ct
	DROP TABLE #Data
	DROP TABLE #Phys_Ct0
	DROP TABLE #Ct0
	DROP TABLE #Ct1
END

