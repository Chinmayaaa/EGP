******************************************************************;
** Program: download_apr.sas                                    **;
**                                                              **;
** Purpose: Download values from the DB2 table oyt_pj_hist_smry **;
**          to allow calculation of apr and tran_adr values.    **;
**                                                              **;
** Pre-reqs: Libref pointed to by db2lib parameter should       **;
**           alread be assigned. All properties for which data  **;
**           should be read should be listed in a number of     **;
**           of 'pclist_' macro variables assigned by the       **;
**           the calling program.                               **;
**                                                              **;
** By:       Yunhong Liao, Andrew Hamilton, June 2014.          **;
**                                                              **;
******************************************************************;

%macro download_apr(db2lib,
                  startdt9,
                  lastdt9,
                  prop_code_ds,
                  outdsn);

   
    %let errflg = 0;


    proc sql;

        create table &outdsn
        as select 
        phs_prop_code as prop_code, 
        phs_inv_date as inv_date,
        phs_currency_code as currency,
        PHS_TRAN_REV/PHS_TRAN_RMS as tran_adr,
        sum(PHS_GRP_REV, PHS_TRAN_REV)/
        sum(PHS_GRP_RMS, PHS_TRAN_RMS) as apr
        from &db2lib..oyt_pj_hist_smry

        where phs_prop_code in (  &pclist_reg_1
        %do j = 2 %to &num_pc_lists ;
             %if %length(&&pclist_reg_&j) > 1 %then
             , &&pclist_reg_&j ;
        %end;

        )
        and phs_inv_date between "&startdt9"D and "&lastdt9"D
        order by prop_code, inv_date
        ;

    quit;


    %dataobs(&outdsn);
    %if &dataobs = 0 %then %do;
         %let errflg = 1;
         %let errmsg = No records output from DB2 table OYT_PJ_HIST_SMRY ;
    %end;


    %submacrend:

%mend;
