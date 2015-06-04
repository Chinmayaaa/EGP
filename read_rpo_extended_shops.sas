******************************************************************;
** Program: read_rpo_extended_shops.sas                         **;
**                                                              **;
** Purpose: Read rpo extended shops and create data to be       **;
**          inserted to the oyt_pr_apr_adjust table with        **;
**          proc_id values of 'RPOExtShp' and adj_type value    **;
**          of 'R'.                                             **;
**                                                              **;
** Pre-reqs: Libref pointed to by db2lib parameter should       **;
**           alread be assigned. All properties for which data  **;
**           should be read should be listed in a number of     **;
**           of 'pclist_' macro variables assigned by the       **;
**           the calling program.                               **;
**                                                              **;
** By:       Julia Morrison, Andrew Hamilton, July 2014.        **;
**                                                              **;
******************************************************************;

%macro read_rpo_extended_shops (
            prop_list_ds,
            oralib,
            orauser,
            orapasswd,
            orapath,
            db2lib,
            lastdt9,
            kdate9,
            rpo_rule_ids,
            default_adj_value,
            rpo_paa_adj_type,
            rpo_paa_proc_id,
            fill_paa_adj_type,
            fill_paa_proc_id, 
            outputdsn,
            vat_col
            );


    %global num_rpo_pc_lists ;

    %let dataobs = 1;
    %let loop = 1;
    %let num_pc_lists = 1;

    %do %while (&dataobs > 0);

        %let lastiter = %eval(&loop -1);

        data rpo_prop_list_&loop ;
            length pcstr $10000;
            set &prop_list_ds._&lastiter end=eof;

            retain pcstr ;

            if _n_ > 1 and _n_ <= 1000 then pcstr = trimn(pcstr) ||"," ;

            if _n_ <= 1000 then pcstr = trimn(pcstr) || compress("'"|| prop_code ||"'");
            else output ;

            if _n_ = 1000 or eof then do;
                call symput(compress("rpo_pclist_&loop"), trimn(pcstr)) ;
                if eof then call symput('num_rpo_pc_lists', compress(put(&loop, 4.)));
            end; 

            drop pcstr ; 
        run;

        
        %dataobs(rpo_prop_list_&loop) ;
        %let loop = %eval(&loop + 1);

    %end;

    %put num_rpo_pc_lists = &num_rpo_pc_lists;
        

    %do j = 1 %to &num_rpo_pc_lists;

        proc sql;

            connect to oracle (user=&orauser password="&orapasswd" path="&orapath") ;

            create table Oracle_rates_&j 
            as select * from connection to oracle (
               select a.prop_code, 
                  a.stydt as stydttm, 
                  case when ((rt < 0) and (rate > 0)) then rate
                  when ((rt < 0) and ((rate is null) or (rate <= 0))) then null
                  when (rt > 0) then rt end as rt
                from rpo_gpo_mi_rate_shop_dly a,  
                     rpo_closed_fill_in b
                where a.prop_code in (&&rpo_pclist_&j)
                  and a.prop_code = b.prop_code
                  and a.shop_dt = (select max(shop_dt) 
                                   from rpo_gpo_mi_rate_shop_dly)
                  and a.shop_dt = b.data_date
                  and a.stydt = b.stay_date );

             disconnect from oracle;

         quit;

     %end;

    
     data oracle_rates ;
         set 
             %do j = 1 %to &num_rpo_pc_lists;
                 oracle_rates_&j 
             %end;
          ;

          format stay_date date10.;
          stay_date = datepart(stydttm);
          drop stydttm;
     run; 

    

    **** get VAT data ****;

    proc sql;
        create table vat_rates 
        as select 
        PGV_PROP_CODE, 
        PGV_START_DATE, 
        PGV_END_DATE, 
        &vat_col as VAT 
        from &db2lib..OYT_PR_GPO_VAT 
        order by PGV_PROP_CODE, 
                 PGV_START_DATE;

    quit;
 

    proc sort data=Oracle_rates;
        by prop_code stay_date;
    run;

   
    proc sql;

        create table APR_ADJUST_TEMPL as
        select * from &db2lib..OYT_PR_APR_ADJUST 
        where PAA_PROP_CODE='Z';

    quit;



    data _null_;
        now = datetime();
        hour = hour(now);
        min = minute(now);
        format now datetime.;
        call symput('now', now);
        call symput('hour', hour);
        call symput('min', min);
    run;
 
 

    data APR_ADJUST_TEMPL2;
        merge APR_ADJUST_TEMPL 
              &prop_list_ds._0;

        do i = "&lastdt9"d to "&kdate9"d;
            PAA_PROC_ID = "&fill_paa_proc_id";
            PAA_TZR = tzr;
            PAA_PROP_CODE = prop_code;
            PAA_STAY_DATE= i;
            PAA_ADJ_TYPE = "&fill_paa_adj_type";
            PAA_ADJ_VALUE = &default_adj_value;
            PAA_LAST_UPD_TS = &now;
            output;
        end;
        drop i;
    run;


    proc sort data = apr_adjust_templ2 ;
        by paa_prop_code paa_stay_date ;
    run;



    data vat_rates2;
        set vat_rates;
        format PAA_STAY_DATE date9.;
        do i = "&lastdt9"d to "&kdate9"d;
            if (i >=  PGV_START_DATE and i<= PGV_END_DATE) then do;
                PAA_STAY_DATE= i;
                output;
            end;
        end;
        keep PAA_STAY_DATE PGV_PROP_CODE VAT;
    run;


    proc sort data = vat_rates2;
       by pgv_prop_code paa_stay_date ;
    run;



    ** This is a file from today to k-DATE with RPO rates **;
    ** to be loaded into the APR_ADJUST table             **;

    data &outputdsn;
        merge APR_ADJUST_TEMPL2 (in= in1) 
              Oracle_rates (rename = (prop_code = PAA_PROP_CODE stay_date=PAA_STAY_DATE)) 
              vat_rates2   (rename = (PGV_PROP_CODE =PAA_PROP_CODE));

         by PAA_PROP_CODE PAA_STAY_DATE;
         if in1;

         if rt ne . then do;
             PAA_ADJ_TYPE = "&rpo_paa_adj_type";
             PAA_ADJ_VALUE = rt;
             PAA_PROC_ID= "&rpo_paa_proc_id";
         
             if vat ne . then 
              PAA_ADJ_VALUE = rt/(1+vat/100.0);
         end;

         drop prop_code tzr vat rt; 
    run;     

%mend;
