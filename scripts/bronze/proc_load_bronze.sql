/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


create or alter procedure bronze.load_bronze as
	begin
            declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;  
			begin try
			    set @batch_start_time=GETDATE();
				print '=============================';
				print 'Loading bronze layer';
				print '=============================';


				print '-------------------------------';
				print 'Loading crm tables '
				print '-------------------------------';

				print '----------------------------------------';
				print '>>Truncating table : bronze.crm_cust_info';
				print '----------------------------------------';
				set @start_time=getdate();
				truncate table bronze.crm_cust_info;

				print '-----------------------------------------';
				print '>>Inserting data into : bronze.crm_cust_info';
				print '------------------------------------------';
				bulk insert bronze.crm_cust_info
				from 'C:\Users\jakki\OneDrive\Desktop\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
				with (
					firstrow=2,
					fieldterminator=','
				);
				set @end_time=GETDATE();
				print 'Time duration : '+cast(datediff(second,@start_time,@end_time)as varchar) + 'seconds'


				print '----------------------------------------';
				print '>>Truncating table : bronze.crm_prd_info';
				print '----------------------------------------';
				set @start_time=getdate();
				truncate table bronze.crm_prd_info;

				print '-----------------------------------------';
				print '>>Inserting data into : bronze.crm_prd_info';
				print '------------------------------------------';
				bulk insert bronze.crm_prd_info
				from 'C:\Users\jakki\OneDrive\Desktop\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
				with (
					firstrow=2,
					fieldterminator=','
				)
				set @end_time=GETDATE();
				print 'Time duration : '+cast(datediff(second,@start_time,@end_time) as varchar) + 'seconds'



				print '----------------------------------------';
				print '>>Truncating table : bronze.crm_sales_details';
				print '----------------------------------------';
				set @start_time=getdate();
				truncate table bronze.crm_sales_details;

				print '-----------------------------------------';
				print '>>Inserting data into : bronze.crm_sales_detials';
				print '------------------------------------------';
				bulk insert bronze.crm_sales_details
				from 'C:\Users\jakki\OneDrive\Desktop\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
				with (
					firstrow=2,
					fieldterminator=','
				)
				set @end_time=GETDATE();
				print 'Time duration : '+cast(datediff(second,@start_time,@end_time) as varchar) + 'seconds'



				print '------------------------------------------';
				print 'Loading ERP tables';
				print '------------------------------------------';


				print '----------------------------------------';
				print '>>Truncating table : bronze.erp_cust_az12';
				print '----------------------------------------';
				set @start_time=getdate();
				truncate table bronze.erp_cust_az12

				print '-----------------------------------------';
				print '>>Inserting data into : bronze.erp_cust_az12';
				print '------------------------------------------';
				bulk insert bronze.erp_cust_az12
				from 'C:\Users\jakki\OneDrive\Desktop\sql\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
				with(
					firstrow=2,
					fieldterminator=','
				)
				set @end_time=GETDATE();
				print 'Time duration : '+cast(datediff(second,@start_time,@end_time) as varchar) + 'seconds'


				print '----------------------------------------';
				print '>>Truncating table : bronze.erp_loc_a101';
				print '----------------------------------------';
				set @start_time=getdate();
				truncate table bronze.erp_loc_a101
				print '-----------------------------------------';
				print '>>Inserting data into : bronze.erp_loc_a101';
				print '------------------------------------------';
				bulk insert bronze.erp_loc_a101
				from 'C:\Users\jakki\OneDrive\Desktop\sql\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
				with (
					firstrow=2,
					fieldterminator=','
				)
				set @end_time=GETDATE();
				print 'Time duration : '+cast(datediff(second,@start_time,@end_time) as varchar) + 'seconds'


				print '----------------------------------------';
				print '>>Truncating table : bronze.erp_px_cat_g1v2';
				print '----------------------------------------';
				set @start_time=getdate();
				truncate table bronze.erp_px_cat_g1v2

				print '-----------------------------------------';
				print '>>Inserting data into : bronze.erp_px_cat_g1v2';
				print '------------------------------------------';
				bulk insert bronze.erp_px_cat_g1v2
				from 'C:\Users\jakki\OneDrive\Desktop\sql\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
				with (
					firstrow=2,
					fieldterminator=','
				)
				set @end_time=GETDATE();
				print 'Time duration : '+cast(datediff(second,@start_time,@end_time) as varchar) + 'seconds'
				set @batch_end_time=GETDATE();
				print '----------------------'
				print 'Time duration for totalbronze load : ' + cast(datediff(second,@batch_start_time,@batch_end_time)as varchar);
			end try
		begin catch
			print 'error occuared';
			print 'error message : '+ error_message();
			print 'erron number : ' + cast(error_number() as varchar);
			print 'error line : '+ cast(error_line() as varchar);
		end catch 
	end


  
exec bronze.load_bronze

