--=================================================================
-- Author: HoangDM
-- Created: 27/07/2024
/* 
Description: This procedure is designed to support the use of Zebra scanners for scanning packages before they are loaded onto delivery vehicles.
The system automatically deducts the scanned items from inventory, enabling real-time tracking of the remaining stock for each product in the warehouse.
*/
--=================================================================
ALTER PROCEDURE dbo.proc_scan_to_export_item
	@_IdList	XML	= NULL,
	@_IdListPhys	XML	= NULL,
	@_DocCode VARCHAR(2) = '',
	@_QRCode NVARCHAR(24) = '',
	@_DocGroup TINYINT = 1,
	@_DocDate DATE = '2021-01-01',
	@_BranchCode VARCHAR(3) = '' ,
	@_FiscalYear NVARCHAR(4) = ''
AS
BEGIN
  SET NOCOUNT ON;
	IF @_IdList IS NULL RETURN;

	DECLARE @_docHandle INT, @_StrExec NVARCHAR(MAX), @_ErrorMessage NVARCHAR(MAX) = '', @_MaxBuiltinOrder INT
		, @_MaxBuiltinOrder_phys INT, @_UseFactoryInventory TINYINT

	SET @_UseFactoryInventory = 0

	EXEC sp_xml_preparedocument @_docHandle OUTPUT, @_IdList; --Lấy dữ liệu theo kiểu nodes thay vì openxml nhanh hơn
	
	SELECT
		x.item.value('@Id[1]', 'INT') AS Id
		,x.item.value('@ItemId[1]', 'INT') AS ItemId
		,x.item.value('@WarehouseId[1],', 'INT') AS WarehouseId
		,x.item.value('@BizDocId_IO[1]', 'VARCHAR(16)') AS BizDocId_IO
		,x.item.value('@RowId_IO[1]', 'VARCHAR(16)') AS RowId_IO
		,x.item.value('@BizDocId_SO[1]', 'VARCHAR(16)') AS BizDocId_SO
		,x.item.value('@RowId[1]', 'VARCHAR(16)') AS RowId
		,x.item.value('@Quantity9[1]', 'NUMERIC(15,4)') AS Quantity9
		,x.item.value('@Quantity[1]', 'NUMERIC(15,4)') AS Quantity
		,x.item.value('@BuiltinOrder[1]', 'INT') AS BuiltinOrder
		,x.item.value('@RequestQuantity[1]', 'NUMERIC(15,4)') AS RequestQuantity
		,x.item.value('@ConvertRate9[1]', 'NUMERIC(15,4)') AS ConvertRate9
		,x.item.value('@IsPromotion[1]', 'TINYINT') AS IsPromotion
		,x.item.value('@OriginalUnitPrice[1]', 'NUMERIC(15,4)') AS OriginalUnitPrice
	INTO #Ct0
	FROM @_IdList.nodes('//NewDataSet//Ct0') AS x(item)

	--UPDATE #Ct0 SET WarehouseId = '' WHERE WarehouseId IS NULL
	------------------------------------------------------------------------------------------------------------
	EXEC sp_xml_preparedocument @_docHandle OUTPUT, @_IdListPhys;
	
	SELECT
		x.item.value('@Id[1]', 'INT') AS Id
		,x.item.value('@ItemId[1]', 'INT') AS ItemId
		,x.item.value('@RollCode[1],', 'NVARCHAR(24)') AS RollCode
		,x.item.value('@Quantity9[1]', 'NUMERIC(15,4)') AS Quantity9
		,x.item.value('@Quantity[1]', 'NUMERIC(15,4)') AS Quantity
		,x.item.value('@_ParentIdentityKey[1]', 'INT') AS _ParentIdentityKey
		,x.item.value('@RowId_SourceDoc[1]', 'VARCHAR(16)') AS RowId_SourceDoc
		,x.item.value('@WarehouseId[1]', 'INT') AS WarehouseId
		,x.item.value('@BuiltinOrder[1]', 'INT') AS BuiltinOrder
	INTO #CtPhys
	FROM @_IdListPhys.nodes('//NewDataSet//phys') AS x(item)

	EXEC sp_xml_removedocument @_docHandle;
	------------------------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS #_CurrentRollValues --tồn tức thời cuộn 
	SELECT WarehouseId, ItemId, ItemCode AS RollCode, Quantity
	INTO #_CurrentRollValues
	FROM  dbo.B00CtTmp

	IF NOT EXISTS (SELECT * FROM  dbo.B30CurrentInventoryPhys WHERE RollCode = @_QRCode AND Quantity > 0) OR @_DocCode = 'SI'
    SET @_UseFactoryInventory = 1

	IF ISNULL(@_UseFactoryInventory,0) = 1 AND @_DocGroup = 2
	BEGIN
	  INSERT INTO #_CurrentRollValues(WarehouseId, ItemId, RollCode, Quantity)
		SELECT WarehouseId, ItemId, RollCode, Quantity
		FROM dbo.B30AccDocItem1	
		WHERE RollCode = @_QRCode 
    AND RowId NOT IN 
					(
            SELECT t1.RowId_TK 
					  FROM dbo.B30AccDocInventoryPhys t1
						  LEFT OUTER JOIN dbo.B30AccDocItem t2 ON t1.Stt = t2.Stt
  						LEFT OUTER JOIN dbo.B00DocStatus t3 ON t2.DocStatus = t3.DocStatusKey
					  WHERE t2.DocCode = 'TP' AND t3.Post_TheKho = 1 AND t2.IsActive = 1 AND t1.RowId_TK IS NOT NULL
          ) 
	END	
	ELSE	
	IF (@_DocGroup = 1)
  BEGIN
	  INSERT INTO #_CurrentRollValues(WarehouseId, ItemId, RollCode, Quantity)
    SELECT NULL, ItemId, Code, 0
		FROM dbo.B20PaperRoll 
		WHERE Code = @_QRCode
	END
	ELSE 
	IF (@_DocGroup = 2) AND ISNULL(@_UseFactoryInventory,0) = 0
	BEGIN
	  SET @_StrExec = 
		N'
  		INSERT INTO #_CurrentRollValues(WarehouseId, ItemId, RollCode, Quantity)
  		SELECT TOP 1 t1.WarehouseId, t1.ItemId, t1.RollCode, t1.Quantity
  		FROM dbo.B30CurrentInventoryPhys t1
  			LEFT OUTER JOIN B20PaperRoll t2 ON t1.RollCode = t2.Code
  		WHERE t1.Quantity >= 0 AND t1.FiscalYear = ''' + @_FiscalYear + ''' AND t1.BranchCode = ''' + @_BranchCode + '''
  			AND t1.RollCode = '''+@_QRCode+'''
  		ORDER BY t1.Quantity DESC'
		EXECUTE sp_executesql @_StrExec
	END
    
	IF EXISTS (SELECT * FROM #CtPhys WHERE RollCode = @_QRCode)
	BEGIN
	    SET @_ErrorMessage = N' 
Cuộn '''+@_QRCode+ N''' đã được quét !'
		RAISERROR(@_ErrorMessage, 16,1);	
		RETURN
	END
	ELSE
	IF NOT EXISTS (SELECT * FROM #_CurrentRollValues WHERE Quantity > 0) 
		AND (SELECT Nh_Ct FROM  dbo.B00DmCt WHERE Ma_Ct = @_DocCode)  = 2 
	BEGIN
		SELECT @_ErrorMessage = N' 
Cuộn '''+@_QRCode+N''' không còn tồn kho để xuất !'
	    RAISERROR(@_ErrorMessage, 16,1);
		RETURN
	END	

	UPDATE #Ct0 SET IsPromotion = 0 WHERE IsPromotion IS NULL
      
	UPDATE t1
	SET WarehouseId = t2.WarehouseId
	FROM #Ct0 t1 
		INNER JOIN #_CurrentRollValues t2 ON t1.ItemId = t2.ItemId 
		LEFT OUTER JOIN dbo.B20Warehouse t3 ON t2.WarehouseId = t3.Id 
	WHERE t1.WarehouseId IS NULL AND t3.IsFactoryWarehouse = 0
	-----------------------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS #Ct0_Ins
	SELECT t1.Id, t1.ItemId, t1.WarehouseId
      , t1.BizDocId_IO, t1.RowId_IO, t1.BizDocId_SO, t1.Quantity9, t1.Quantity, t1.BuiltinOrder
      , t1.RequestQuantity, t1.ConvertRate9, t1.IsPromotion, t1.OriginalUnitPrice 
		  , t1.RowId
		  , IIF(t1.RequestQuantity IS NULL, 0,IIF(ISNULL(t1.Quantity9,0) < t1.RequestQuantity, 0, 1)) Enough --Request null trong TH ko có lệnh
		  , RANK() OVER(PARTITION BY t1.ItemId ORDER BY t1.IsPromotion, t1.BuiltinOrder) AS ScanOrder --Thứ tự đẩy phys
 	INTO #Ct0_Ins FROM #Ct0 t1

	UPDATE t1 
	SET t1.ScanOrder = ISNULL(t2.ScanOrder,0)
	FROM #Ct0_Ins t1
		LEFT OUTER JOIN 
		(
			SELECT a.ItemId, a.BuiltinOrder
				, RANK() OVER(PARTITION BY a.ItemId ORDER BY a.IsPromotion, a.BuiltinOrder) AS ScanOrder 
			FROM #Ct0_Ins a 
				INNER JOIN dbo.B20Warehouse b ON a.WarehouseId = b.Id
			WHERE b.IsFactoryWarehouse = 0
		) t2 ON t1.ItemId = t2.ItemId AND t1.BuiltinOrder = t2.BuiltinOrder
	-----------------------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS #Phys_Ins
	SELECT Id, ItemId, RollCode,  Quantity9, Quantity
		, _ParentIdentityKey, WarehouseId, BuiltinOrder, RowId_SourceDoc
	INTO #Phys_Ins
	FROM #CtPhys
	-----------------------------------------------------------------------------------------------------------
	SELECT @_MaxBuiltinOrder = ISNULL(MAX(BuiltinOrder), 0) FROM #Ct0
	SELECT @_MaxBuiltinOrder_phys = ISNULL(MAX(BuiltinOrder), 0) FROM #Phys_Ins

	DECLARE @_MaxId_Ct0 INT
	SELECT @_MaxId_Ct0 = ISNULL(MAX(ABS(Id)),0) FROM #Ct0_Ins
	
	DECLARE @_MaxIdPhys INT 
	SELECT @_MaxIdPhys = MAX(ABS(Id)) FROM #Phys_Ins
	-----------------------------------------------------------------------------------------------------------
	IF NOT EXISTS (SELECT * FROM  #_CurrentRollValues t1 INNER JOIN #Ct0_Ins t2 ON (t1.ItemId = t2.ItemId 
		AND t1.WarehouseId = t2.WarehouseId AND @_DocGroup = 2) --Xuất kho
		OR (t1.ItemId = t2.ItemId AND @_DocGroup = 1)) --Nhập
	BEGIN	
		INSERT INTO #Ct0_Ins(Id, ItemId, WarehouseId, Quantity9, Quantity, BuiltinOrder, IsPromotion, Enough, ScanOrder)
		SELECT IIF(ISNULL(@_MaxId_Ct0,0)=0,1,ABS(@_MaxId_Ct0)+1)*-1 AS Id
			, a.ItemId, a.WarehouseId, a.Quantity, a.Quantity, @_MaxBuiltinOrder + 1
			, 0, 0, 1
		FROM #_CurrentRollValues a 	

		INSERT INTO #Phys_Ins(ItemId, WarehouseId, RollCode, Quantity9, Quantity, _ParentIdentityKey, BuiltinOrder)
		SELECT t1.ItemId, t1.WarehouseId, t1.RollCode, t1.Quantity, t1.Quantity
			, IIF(ISNULL(@_MaxId_Ct0,0)=0,1,ABS(@_MaxId_Ct0)+1)*-1, @_MaxBuiltinOrder_phys + 1
		FROM #_CurrentRollValues t1

		UPDATE #Phys_Ins
		SET	Id = t2.Id*-1
		FROM #Phys_Ins t1
			INNER JOIN 
				(
					SELECT ItemId, ISNULL(@_MaxIdPhys,0) + ROW_NUMBER() OVER(ORDER BY ItemId) Id 
					FROM #Phys_Ins WHERE Id IS NULL
				) t2 ON t2.ItemId = t1.ItemId
		WHERE t1.Id IS NULL

		UPDATE t1
		SET t1.Quantity9 =  t2.Quantity9,
			t1.Quantity = t2.Quantity
		FROM #Ct0_Ins t1 
		  INNER JOIN 
			(
				SELECT SUM(Quantity9) AS Quantity9, SUM(Quantity) AS Quantity, _ParentIdentityKey
				FROM #Phys_Ins
				GROUP BY _ParentIdentityKey
			) t2 ON t1.Id = t2._ParentIdentityKey

		SELECT t1.Id, t1.ItemId, t1.WarehouseId, t1.BizDocId_IO,
               t1.RowId_IO, t1.BizDocId_SO, t1.Quantity9, t1.Quantity,
               t1.BuiltinOrder, t1.RequestQuantity, t1.IsPromotion,
               t1.OriginalUnitPrice, ISNULL(t1.RowId,'') AS RowId, t1.Enough,
               t1.ScanOrder, @_DocCode AS DocCode, @_DocDate AS DocDate, @_DocGroup AS DocGroup
		FROM #Ct0_Ins t1

		SELECT Id, ItemId, RollCode, Quantity9, Quantity,
               _ParentIdentityKey, WarehouseId,
               BuiltinOrder, ISNULL(RowId_SourceDoc,'') AS RowId_SourceDoc, 1 AS _PhysConvertingFlag 
			, @_DocDate AS DocDate, @_DocGroup AS DocGroup, @_DocCode AS DocCode
		FROM  #Phys_Ins
		GOTO _Drop
	END

	DECLARE @_ItemId INT, @_OtherUnit TINYINT = 0 
	SELECT @_ItemId = ItemId FROM dbo.B30CurrentInventoryPhys WHERE RollCode = @_QRCode AND Quantity > 0
	
	ALTER TABLE #_CurrentRollValues ADD Quantity9 NUMERIC(18,2), Packing NUMERIC(18,2)

	IF @_DocCode IN ('XK','HD','DC') 
	AND EXISTS (SELECT * FROM #Ct0_Ins t1 INNER JOIN dbo.B20Item t2 ON t1.ItemId = t2.Id WHERE t1.ConvertRate9 <> 1
		AND t1.ItemId = @_ItemId
		)
	BEGIN
		UPDATE t1
		SET	t1.Quantity9 = t1.Quantity / t2.Packing, t1.Packing = t2.Packing
		FROM #_CurrentRollValues t1 
			LEFT OUTER JOIN dbo.B20Item t2 ON t1.ItemId = t2.Id 
      
		SET @_OtherUnit = 1
	END

	IF @_DocGroup = 2 
	BEGIN	
		;WITH Distribute AS (
			SELECT TOP 1 t1.Id, t1.ItemId, t1.WarehouseId, t1.BizDocId_IO, t1.RowId_IO, t1.BizDocId_SO, t1.Quantity9
				, t1.RequestQuantity
				, CAST(LEAST(t1.RequestQuantity - ISNULL(t1.Quantity9,0), IIF(@_OtherUnit = 0, t2.Quantity, ISNULL(t2.Quantity9,0))) AS NUMERIC(18,2)) AS Allocated
				, CAST(t2.Quantity - LEAST(t1.RequestQuantity - ISNULL(t1.Quantity9,0),IIF(@_OtherUnit = 0,t2.Quantity, ISNULL(t2.Quantity9,0))) AS NUMERIC(18,2)) AS Remain
				, t1.RowId
				, @_QRCode AS RollCode, t1.ScanOrder
			FROM #Ct0_Ins t1
				JOIN #_CurrentRollValues t2 ON t2.ItemId = t1.ItemId AND t1.WarehouseId = t2.WarehouseId
			WHERE t1.ItemId = @_ItemId AND t1.Enough = 0
			ORDER BY t1.IsPromotion, t1.ScanOrder	
			UNION ALL
			SELECT t2.Id, t2.ItemId, t2.WarehouseId, t2.BizDocId_IO, t2.RowId_IO, t2.BizDocId_SO, t2.Quantity9
				, t2.RequestQuantity
				, CAST(LEAST(t2.RequestQuantity - ISNULL(t2.Quantity9,0), t1.Remain) AS NUMERIC(18,2)) AS Allocated
				, CAST(t1.Remain - LEAST(t2.RequestQuantity - ISNULL(t2.Quantity9,0), t1.Remain) AS NUMERIC(18,2)) AS Remain
				, t2.RowId
				, @_QRCode, t1.ScanOrder + 1
			FROM Distribute t1
				JOIN #Ct0_Ins t2 ON t2.ScanOrder = t1.ScanOrder + 1 AND t2.ItemId = t1.ItemId AND t1.WarehouseId = t2.WarehouseId
			WHERE t1.Remain > 0
		)
		INSERT INTO #Phys_Ins( ItemId, WarehouseId, RollCode, Quantity9, Quantity, _ParentIdentityKey, BuiltinOrder, RowId_SourceDoc)
		SELECT t1.ItemId, t1.WarehouseId, t1.RollCode, t1.Allocated, (t1.Allocated * ISNULL(t2.Packing,1)), t1.Id, @_MaxBuiltinOrder_phys + 1, t1.RowId
		FROM Distribute t1
			LEFT OUTER JOIN #_CurrentRollValues t2 ON t1.RollCode = t2.RollCode
	END
	ELSE	
	IF @_DocGroup = 1 --Nhập thì auto đẩy số lượng = 0
	BEGIN	
	  INSERT INTO #Phys_Ins( ItemId, WarehouseId, RollCode, Quantity9, Quantity, _ParentIdentityKey, BuiltinOrder, RowId_SourceDoc)
		SELECT t1.ItemId, NULL, t2.RollCode, 0, 0, t1.Id, @_MaxBuiltinOrder_phys + 1, t1.RowId
		FROM #Ct0_Ins t1
			LEFT OUTER JOIN #_CurrentRollValues t2 ON t1.ItemId = t2.ItemId;
	END

	UPDATE #Phys_Ins
	SET	Id = t2.Id*-1
	FROM #Phys_Ins t1
		INNER JOIN 
			(
				SELECT ItemId, _ParentIdentityKey, ISNULL(@_MaxIdPhys,0) + ROW_NUMBER() OVER(ORDER BY ItemId) Id 
				FROM #Phys_Ins WHERE Id IS NULL
			) t2 ON t2.ItemId = t1.ItemId AND t1._ParentIdentityKey = t2._ParentIdentityKey
	WHERE t1.Id IS NULL

	UPDATE t1
	SET t1.Quantity9 =  t2.Quantity9,
		t1.Quantity = t2.Quantity
	FROM #Ct0_Ins t1 
		INNER JOIN 
		(
			SELECT SUM(Quantity9) AS Quantity9, SUM(Quantity) AS Quantity, _ParentIdentityKey
			FROM #Phys_Ins
			GROUP BY _ParentIdentityKey
		) t2 ON t1.Id = t2._ParentIdentityKey

	SELECT t1.*, @_DocCode AS DocCode, ISNULL(t1.RowId, '') AS RowId
		, @_DocDate AS DocDate, @_DocGroup AS DocGroup
	FROM #Ct0_Ins t1
	ORDER BY t1.BuiltinOrder

	SELECT Id, ItemId, RollCode, Quantity9 AS Quantity9, Quantity
		, _ParentIdentityKey, WarehouseId, 1 AS _PhysConvertingFlag, BuiltinOrder	
		, ISNULL(RowId_SourceDoc,'') AS RowId_SourceDoc
		, @_DocDate AS DocDate, @_DocGroup AS DocGroup, @_DocCode AS DocCode
	FROM #Phys_Ins
	ORDER BY BuiltinOrder
	
	_Drop:
	DROP TABLE #Ct0
	DROP TABLE #CtPhys
	DROP TABLE #Ct0_Ins
	DROP TABLE #Phys_Ins
	DROP TABLE #_CurrentRollValues
END
