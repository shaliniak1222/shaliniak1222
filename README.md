USE [Transaction]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CWT_GetMFExecutionDetailsInfo]') AND type in (N'P', N'PC'))
BEGIN
DROP PROCEDURE [dbo].[CWT_GetMFExecutionDetailsInfo]
End
GO

/****** Object:  StoredProcedure [dbo].[CWT_GetMFExecutionDetailsInfo]    Script Date: 01/05/2017 09:36:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[CWT_GetMFExecutionDetailsInfo]
@OrderNumber DECIMAL,
@numDays Integer = 7 
AS


---==============================================================================  
--@ Stored Procedure: [CWT_GetMFExecutionDetailsInfo]   
--------------------------------------------------------------------------------  
--@ Description:  
--  Used for Loading MFExecution RTP data to update Coherence Cache  
--------------------------------------------------------------------------------  
--@ Implementation Notes:  
--  EXEC [Transaction].[dbo].[CWT_GetMFExecutionDetailsInfo] @OrderNumber = 17010000098.0    
--------------------------------------------------------------------------------  
--@ Notes:   
--   
--==============================================================================  
   
BEGIN    
    
DECLARE @StartDate Date     
    
BEGIN TRY    
   
SET @StartDate = Support.dbo.PriorBusinessDays(-@numDays, getdate())  
SELECT  DISTINCT       
  M.ID,  
  bmf.TransactionId as [TransactionId],      
  bmf.asofdate as AsOf,      
  bmf.AccountNo as AccountNumber,      
  bmf.SecurityNo as SecurityNumber,      
  bmf.OrderStatusCode as OrderStatus,      
  sb.Symbol,      
  sb.Cusip,      
  sb.[Description] as [Description],      
  bmf.BuySellInd as ActionType,      
  bmf.Quantity as [Quantity],      
  bmf.QtyDescInd as [QuantityQualifier],      
  bmf.Note1 as NoteLine1,      
  bmf.Note2 as NoteFrom,      
  bmf.Price as FundPrice,      
  bmf.RepId as RepId,      
  --bmf.OrderEntryDate as OrderDate,      
  [RTP].[dbo].BuildMFTime(bmf.OrderAddedDate, bmf.OrderEntryDate, bmf.TimeOfOrder) as OrderDate,         
  bmf.AccountType,      
  bmf.NSCCControlNoInd as OrderCorrection,      
  bmf.GrossAmount,      
  bmf.NetAmount,      
  bmf.SettleDate as OpenSettleDate,      
  bmf.AsOfReasonCode as AsOfReason,      
  bmf.CancelInd as OrderExit,      
  bmf.BetaRecordNo as OrderNumber,      
  bmf.ExchDaysToSettle as SettledDays,      
  bmf.NSCCStatusCode as NSCCStatusCode,      
  bmf.PIPSModeInd as PIPSSwitch,      
  bmf.Origincode as OriginID,      
  bmf.VSPInd as TaxLotInd,      
  bmf.CancelInd as CancelInd,      
  bmf.NSCCCorrCode as NSCCCorrCode,      
  case when bmf.Price is NULL or bmf.Price = 0 then 'FUND'      
  else 'FECH' end       
  as RecordType,
  bmf.NSCCControlNo,
  bmf.TerminalId,
  bmf.ExchQuantityInd
  INTO #tblMFExecutionDetailsInfo   
  FROM [RTP].[dbo].[Beta_Mq_MFOrderEntry] bmf (nolock)  
  INNER JOIN LPLCustomer.dbo.SecurityBeta sb (nolock)on bmf.securityNo = sb.securityNo 
  LEFT JOIN [Transaction].dbo.MutualFunds M (nolock) ON M.TransactionId = bmf.TransactionId    
WHERE bmf.BetaRecordNo = @OrderNumber      
    
      
IF((select count(*) from #tblMFExecutionDetailsInfo) = 0)      
 BEGIN      
   INSERT INTO #tblMFExecutionDetailsInfo     
   SELECT DISTINCT      
    M.ID,      
    mfoe.TransactionId as [TransactionId],      
    mfoe.asofdate as AsOf,      
    mfoe.AccountNo as AccountNumber,      
    mfoe.SecurityNo as SecurityNumber,      
    mfoe.OrderStatusCode as OrderStatus,      
    sb.Symbol,      
    sb.Cusip,      
    sb.[Description] as [Description],      
    mfoe.BuySellInd as ActionType,      
    mfoe.Quantity as [Quantity],      
    mfoe.QtyDescInd as [QuantityQualifier],      
    mfoe.Note1 as NoteLine1,      
    mfoe.Note2 as NoteFrom,      
    mfoe.Price as FundPrice,      
    mfoe.RepId as RepId,      
    --mfoe.OrderEntryDate as OrderDate,      
	[RTP].[dbo].BuildMFTime(mfoe.OrderAddedDate, mfoe.OrderEntryDate, mfoe.TimeOfOrder) as OrderDate, 
    mfoe.AccountType,      
    mfoe.NSCCControlNoInd as OrderCorrection,      
    mfoe.GrossAmount,      
    mfoe.NetAmount,      
    mfoe.SettleDate as OpenSettleDate,      
    mfoe.AsOfReasonCode as AsOfReason,      
    mfoe.CancelInd as OrderExit,      
    mfoe.BetaRecordNo as OrderNumber,      
    mfoe.ExchDaysToSettle as SettledDays,      
    mfoe.NSCCStatusCode as NSCCStatusCode,      
    mfoe.PIPSModeInd as PIPSSwitch,      
    mfoe.Origincode as OriginID,      
    mfoe.VSPInd as TaxLotInd,      
    mfoe.CancelInd as CancelInd,      
    mfoe.NSCCCorrCode as NSCCCorrCode,  
    case when mfoe.Price is NULL or mfoe.Price = 0 then 'FUND'      
    else 'FECH' end       
    as RecordType,
	mfoe.NSCCControlNo,
	mfoe.TerminalId,
    mfoe.ExchQuantityInd
         
 FROM [Beta].[dbo].[BETA_MUTUAL_FUNDS_ORDER_ENTRY] mfoe (nolock)      
    LEFT JOIN LPLCustomer.dbo.SecurityBeta sb (nolock)on mfoe.securityNo = sb.securityNo       
    LEFT JOIN [Transaction].dbo.MutualFunds M (nolock) ON M.TransactionId = mfoe.TransactionId      
    WHERE mfoe.BetaRecordNo = @OrderNumber        
        
     IF((select count(*) from #tblMFExecutionDetailsInfo)= 0)      
     BEGIN     
     INSERT INTO #tblMFExecutionDetailsInfo    
     SELECT DISTINCT      
     M.ID,      
     hist.TransactionId as [TransactionId],      
     hist.asofdate as AsOf,      
     hist.AccountNo as AccountNumber,      
     hist.SecurityNo as SecurityNumber,      
     hist.OrderStatusCode as OrderStatus,      
     sb.Symbol,      
     sb.Cusip,      
     sb.[Description] as [Description],      
     hist.BuySellInd as ActionType,      
     hist.Quantity as [Quantity],      
     hist.QtyDescInd as [QuantityQualifier],      
     hist.Note1 as NoteLine1,      
     hist.Note2 as NoteFrom,      
     hist.Price as FundPrice,      
     hist.RepId as RepId,      
     --hist.OrderEntryDate as OrderDate,      
	 [RTP].[dbo].BuildMFTime(null, hist.OrderEntryDate, hist.TimeOfOrder) as OrderDate,
     hist.AccountType,      
     hist.NSCCControlNoInd as OrderCorrection,      
     hist.GrossAmount,      
     hist.NetAmount,      
     hist.SettleDate as OpenSettleDate,      
     hist.AsOfReasonCode as AsOfReason,      
     hist.CancelInd as OrderExit,      
     hist.BetaRecordNo as OrderNumber,      
     hist.ExchDaysToSettle as SettledDays,      
     hist.NSCCStatusCode as NSCCStatusCode,      
     hist.PIPSModeInd as PIPSSwitch,      
     hist.Origincode as OriginID,      
     hist.VSPInd as TaxLotInd,      
     hist.CancelInd as CancelInd,      
     hist.NSCCCorrCode as NSCCCorrCode,     
     case when hist.Price is NULL or hist.Price = 0 then 'FUND'      
     else 'FECH' end       
     as RecordType,
	 hist.NSCCControlNo,
	 hist.TerminalId,
     hist.ExchQuantityInd
     FROM [Beta].[dbo].[Beta_mutual_funds_order_entry_history] hist (nolock)      
     LEFT JOIN LPLCustomer.dbo.SecurityBeta sb (nolock)on hist.securityNo = sb.securityNo      
     LEFT JOIN [Transaction].dbo.MutualFunds M (nolock) ON M.TransactionId = hist.TransactionId       
     WHERE hist.BetaRecordNo = @OrderNumber      
     
     END     
        
 END      
    
SELECT [ID],[TransactionId],[AsOf],[AccountNumber],[SecurityNumber],[OrderStatus],[Symbol],[Cusip],[Description],  
[ActionType],[Quantity],[QuantityQualifier],[NoteLine1],[NoteFrom],[FundPrice],[RepId],[OrderDate],  
[AccountType],[OrderCorrection],[GrossAmount],[NetAmount],[OpenSettleDate],[AsOfReason],[OrderExit],[OrderNumber],  
[SettledDays],[NSCCStatusCode],[PIPSSwitch],[OriginID],[TaxLotInd],[CancelInd],[NSCCCorrCode],[RecordType],[NSCCControlNo],[TerminalId],[ExchQuantityInd]
 FROM #tblMFExecutionDetailsInfo  tabMFExecution1  
WHERE EXISTS(SELECT 1  
         FROM #tblMFExecutionDetailsInfo tabMFExecution2  
         WHERE tabMFExecution2.OrderNumber = tabMFExecution1.OrderNumber  
         GROUP BY tabMFExecution2.OrderNumber  
         HAVING tabMFExecution1.AsOf = MAX(tabMFExecution2.AsOf))  
  
 END TRY    
    
 BEGIN CATCH    
   DECLARE @ErrorNumber int, @ErrorSeverity int, @ErrorState int, @ErrorMessage nvarchar(4000)     
   SELECT @ErrorNumber = error_number(), @ErrorMessage = error_message(), @ErrorSeverity = error_severity(), @ErrorState = error_state();    
   RAISERROR (N'<<Error No: %d>> <<Error Msg: %s>>', @ErrorSeverity, @ErrorState, @ErrorNumber, @ErrorMessage)   
 END CATCH    
   
END     

go
