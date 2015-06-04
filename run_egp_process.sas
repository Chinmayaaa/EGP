******************************************************************************;
** Program: run_egp_process.sas                                             **;
** Purpose: This program initializes necessary conditions and calls sub-    **;
**          programs of the GPO EGP process.                                **;
**                                                                          **;
** By:      Andrew Hamilton, Jan 30th 2014.                                 **;
**                                                                          **;
**                                                                          **;
******************************************************************************;

%macro run_egp_process (config_file_path, config_file_name, tzr);


    %let errflg=0;
    %let num_reg_failed_props=0;
    %let num_def_rp_rate_props=0;


    **************************************************;
    ** Initialize the configuration macro variables **;
    **************************************************;


    %global  
    errmsg
    as_of_date
    rpo_kdate_yn
    num_past_years
    data_mart_loc              
    run_reg_yn                 
    remote_yn 
    indep_var
    model_var 
    db2db                      
    db2usr                     
    db2pw                      
    db2schema   
    db2_mmp_schema 
    orausr                     
    orapw                      
    orapath                    
    ntzausr                    
    ntzapwd                    
    ntzasrvr                   
    ntzadb                     
    pr_rule_set_id
    rpo_pr_rule_set_ids
    subset_prop_codes_yn       
    prop_code_input_list_path  
    prop_code_input_list_name 
    exception_properties_yn
    exception_pc_file_loc 
    exception_pc_file_name
    max_num_pcs_each_iter      
    allowed_market_seg_vals
    ab_tier_los_txt_val
    disallowed_bkng_rates 
    rate_outlier_remove_multiplier 
    rp_file_location           
    rp_file_name_prefix 
    adr_default_val
    apr_default_val 
    default_adj_out_value  
    outlier_filter_sd_mult
    max_days_left
    cap_percentiles
    output_data_set_name
    paa_adj_type
    paa_proc_id
    rpo_adj_type
    rpo_proc_id
    rpo_def_adj_value 
    fill_adj_type
    fill_proc_id 
    report_default_codes
    email_addresses
    gpo_vat_col
    default_yearly_pct_increase
    ;


    ************************************;
    ** 1. Read the Configuration File **;
    ************************************;

    ** Assign a filename to the config file **;
    filename confile "&config_file_path/&config_file_name" ;

    %let rc = %sysfunc(fexist(confile)) ;
    %if &rc = 0 %then %do ;
        %let errflg = -1 ;
        %let errmsg = Unable to find the config file &config_file_path\config_file_name ;
        %goto macrend ;
    %end;



    options lrecl=132;

    ** Read the config file settings **;

    data config ;
    
        length keyval $32 char $1 value $100;
        infile confile pad;
        input key $1-32 value $33-132 ;

        ** Deal with bug that inserts phantom characters into the read key value **;
        do j = 1 to length(compress(key)) ;
            char = substr(compress(key),j,1) ;
            r = rank(char) ;
            if r >=46 and r<=122 and not (r>57 and r<65) and (r not in (47, 91, 92, 93, 94, 96)) then
            keyval = compress(keyval) !! char ;
        end;

        if index(value, 'TOP_DIR') > 0 then do;
            topdir_pos = index(compress(value), 'TOP_DIR') ;
            put 'TOP_DIR found in ' value '. Topdir_pos is: ' topdir_pos ;
            if topdir_pos > 1 then 
            value = compress( substr(compress(value),1,(topdir_pos -1)) || "&top_dir" !! 
                              substr(compress(value), topdir_pos +7 ));
            else
            value = compress( "&top_dir" !! substr(compress(value), topdir_pos +7 ));

        end;

        call symput (compress(keyval), trim(left(value))) ;
        
    run;



    ** Check the existance of necessary config values **;
    
    %if %length(as_of_date) = 0                
    or  %length(data_mart_loc) = 0             
    or  %length(run_reg_yn) = 0                
    or  %length(remote_yn) = 0                 
    or  %length(indep_var) = 0                 
    or  %length(model_var) = 0                 
    or  %length(db2db) = 0                     
    or  %length(db2usr) = 0                    
    or  %length(db2pw) = 0                     
    or  %length(db2schema) = 0                 
    or  %length(orausr) = 0                    
    or  %length(orapw) = 0                     
    or  %length(orapath) = 0                   
    or  %length(ntzausr) = 0                   
    or  %length(ntzapwd) = 0                   
    or  %length(ntzasrvr) = 0                  
    or  %length(ntzadb) = 0                    
    or  %length(pr_rule_set_id) = 0  
    or  %length(subset_prop_codes_yn) = 0  
    or  %length(prop_code_input_list_path) = 0
    or  %length(prop_code_input_list_name) = 0
    or  %length(max_num_pcs_each_iter) = 0
    or  %length(allowed_market_seg_vals) = 0
    or  %length(rp_file_location) = 0
    or  %length(rp_file_name_prefix) = 0
    or  %length(adr_default_val) = 0
    or  %length(apr_default_val) = 0
    or  %length(outlier_filter_sd_mult) = 0
    or  %length(max_days_left) = 0
    or  %length(output_data_set_name) = 0
    or  %length(paa_adj_type) = 0
    or  %length(paa_proc_id) = 0
    or  %length(report_default_codes) = 0
    or  %length(email_addresses) = 0
    %then %do;   
        %let errflg = 1;
        %let errmsg = Error: One or more of the necessary configuration options was not found. ;
        %goto macrend;
    %end;
                    
    

    ******************************;
    ** Assign necessary librefs **;
    ******************************;
    
    %if &remote_yn = Y %then %do;

        libname oralref  slibref=oraoys5  server=RemName1;
        libname db2lref  slibref=db2oys5  server=RemName1;
        libname db2mmplf slibref=db2mpoy5 server=RemName1;
      
    %end;
    %else %do;

         libname db2lref DB2 DB = &db2db 
                           USER = &db2usr 
                          USING = "%str(&db2pw)" 
                         SCHEMA = &db2schema ;

         libname db2mmplf DB2 DB = &db2db 
                             USER = &db2usr 
                            USING = "%str(&db2pw)" 
                           SCHEMA = &db2_mmp_schema ;
     
         **
         libname db2temp DB2 DB = &db2db 
                           USER = &db2usr 
                          USING = "%str(&db2pw)"
                     connection = global 
                         SCHEMA = SESSION **;

         libname oralref oracle USER = &orausr 
                            password = "%str(&orapw)" 
                                PATH = &orapath ;

    %end;


    ** Assign an output db2 libref for writing output to the db2 database, **;
    ** irrespective of whether remote_yn is Y or N.                        **;
    libname db2lrout DB2 DB = &db2db 
                       USER = &db2usr 
                      USING = "%str(&db2pw)" 
                     SCHEMA = &db2schema ;


    ** Check the validity of the DB2 libref **;

    %let rc = %sysfunc(libref(db2lref)) ;
    %if &rc > 0 %then %do ;
        %let errflg = 1 ;
        %let errmsg = Unable to assign a libref to the input DB2 library ;
        %goto macrend ;
    %end;

    %if remote_yn = Y %then %do;
        %let rc = %sysfunc(libref(db2lrout)) ;
        %if &rc > 0 %then %do ;
            %let errflg = 1 ;
            %let errmsg = Unable to assign a libref to the output dev DB2 library ;
            %goto macrend ;
        %end;
    %end;



    ** Check the validity of the Oracle libref **;

    %let rc = %sysfunc(libref(oralref)) ;
    %if &rc > 0 %then %do ;
        %let errflg = 1 ;
        %let errmsg = Unable to assign a libref to the input Oracle library ;
        %goto macrend ;
    %end;



    ** Assign the netezza libref **;
    libname netlref netezza user="&ntzausr" password="&ntzapwd"
       server="&ntzasrvr" database="&ntzadb";

    ** Check the validity of the Netezza libref **;

    %let rc = %sysfunc(libref(netlref)) ;
    %if &rc > 0 %then %do ;
        %let errflg = 1 ;
        %let errmsg = Unable to assign a libref to the input Netezza library ;
        %goto macrend ;
    %end;



    ** Assign the data mart location. **;

    libname sasdmart "&data_mart_loc";

    ** Check the validity of the sasdmart libref **;

    %let rc = %sysfunc(libref(sasdmart)) ;
    %if &rc > 0 %then %do ;
        %let errflg = 1 ;
        %let errmsg = Unable to assign a libref to the input/output SAS library ;
        %goto macrend ;
    %end;


    ** Assign a fileref to the file holding exception properties **;

    %if &exception_properties_yn = Y %then %do;

        filename expcfile "&exception_pc_file_loc/&exception_pc_file_name";

        ** Check the fileref **;
        %let rc = %sysfunc(fileref(expcfile)) ;
        %if &rc ne 0 %then %do;
            %let errflg = -1 ;
            %let errmsg = Error: Unable to assign fileref to Exception Properties file. ;
            %let errmsg = &errmsg No exception properties will be processed. ;
            %let num_exception_pcs = 0;
        %end;

        %else %do;

            ** Read the exception property list **;

            data exception_properties;
                length pclist $8000;
                retain pclist ;
                infile expcfile end=endfile_;
                input prop_code $1-5 ;

                if _n_ = 1 then pclist = compress("'"|| prop_code ||"'") ;
                else pclist = trimn(pclist) || compress(",'"|| prop_code ||"'") ;

                if endfile_ then call symput('exception_pc_list', trimn(pclist));
            run;
    
            %dataobs (exception_properties);
            %let num_exception_pcs = &dataobs ;

            %if &dataobs = 0 %then %do;
                %let errflg = -1 ;
                %let errmsg = No property codes read from the Exception Properties file. ;
                %let errmsg = &errmsg No exception properties will be processed. ;
            %end;

            proc sort data = exception_properties ;
                by prop_code ;
            run;

        %end;
    %end;
    %else %let num_exception_pcs = 0;


    ** Assign first and last date for which to read historic data. **;
    ** The last date is also the first date from which the output  **;
    ** dates will be calculated.                                   **;

    %let rpo_asofdate = 0 ;


    ** Read as-of date and Kdate from rpo_parameters **;
    %if &as_of_date = MARSHA or &as_of_date = TODAY %then %do;
  
        data rpo_params1 ;
            set oralref.rpo_parameters ;
            where param_cat = 'SYSTEM' 
              and compress(param_name) = 'BATCH_DATA_DT' 
              and char_val = '3' ;

            call symput('rpo_asofdate', datepart(date_val)) ;
        run;

    %end;



    %if &rpo_kdate_yn = Y %then %do;

        data rpo_params2;
            set oralref.rpo_parameters ;
            where param_cat = 'KDATE' 
              and compress(param_name) = 'KDATE';
            call symput('actual_kdate', put(datepart(date_val), date9.)) ;

            * Add one to the kdate value, since apparently we should use kdate + 1 *;
            * as the first date for kdate through to max days out.                 *;
            call symput('kdate', put(datepart(date_val) +1, date9.)) ;
        run;

    %end;



    %let number_of_seg_vals = 0;
    %let number_of_disallowed_rates = 0; 

    ** Set the minimum and maximum dtm values for which to read reservations data **;
    data _null_ ;
        length seg_commas $200 segval $100 rulecd_commas defcd_commas $40 
               disrates disrt_commas $200 disrt $20 rulecd $2; 

        if "&as_of_date" not in ('TODAY', 'MARSHA') then do;

            %if &as_of_date ne TODAY and &as_of_date ne MARSHA %then %do;
                if index("JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC",
                         substr(compress(upcase("&as_of_date")),3,3)) > 0
                then asofdate = "&as_of_date"d ;
                else asofdate = input("&as_of_date", mmddyy10.);
            %end;
 
            if asofdate ne . then
            first_date = intnx('year', asofdate, (-1 * &num_past_years), 'SAME') ; 
            else do;
                call symput('errflg','1');
            end; 
        end;
        else do;
            asofdate = &rpo_asofdate ;
            first_date = intnx('year', asofdate, (-1 * &num_past_years), 'SAME') ; 
        end;
 
        call symput('first_date', compress(put(first_date, 8.)));
        call symput('first_dtm',  compress(put(first_date * 86400, 12.)));
        call symput('first_dt9',  compress(put(first_date, date9.)));
        call symput('last_date', compress(put(asofdate, 8.)));
        call symput('last_dtm',  compress(put(asofdate * 86400, 12.)));
        call symput('last_dt9',  compress(put(asofdate, date9.)));
        call symput('last_mdy', compress(put(asofdate, mmddyy10.), '/ '));


        ** scan through the allowed netezza segment values **;
        segvals = trim(left("&allowed_market_seg_vals"));
        scan_i = 1;
        seg_commas = '';
        do while (compress(scan(segvals, scan_i, ',')) ne '');
            segval = trim(left(scan(segvals, scan_i, ','))) ;
            call symput(compress('segval_' || put(scan_i,3.)), trimn(segval)) ;
            if scan_i > 1 then seg_commas = trim(left(seg_commas)) ||',' ;
            seg_commas = trimn(seg_commas) ||'"'|| trimn(segval) ||'"' ;
            scan_i + 1 ;       
        end;
        call symput('number_of_seg_vals', put(scan_i - 1, 3.)) ;
        call symput('segval_commas', trim(left(seg_commas))) ;

 
        * Output macro variables holding recipient e-mails.   *;
        * Begin pdf recipient email addresses.                *;
        email_scan_ind = 1;
        if length(compress("&email_addresses")) > 2 then do ;
            do while (compress(scan("&email_addresses", email_scan_ind, ' ')) ne '') ;
                email = compress(scan("&email_addresses", email_scan_ind, ' ')) ;
                call symput('email_'|| left(put(email_scan_ind,3.)), compress(email));
                put email_scan_ind = email = ; 
                email_scan_ind + 1;
            end;
            call symput('num_emails', put(email_scan_ind -1, 4.));
        end;
        else if length(compress("&email_addresses")) <= 2 then do;
            call symput('errmsg', 'No notification e-mail addressed were supplied');
            call symput('num_emails', '0');
        end;
 
        ** Scan through the default rate indicator values for reporting **;
        defcodes = trim(left("&report_default_codes"));
        scan_i = 1;
        defcd_commas = '';
        do while (compress(scan(defcodes, scan_i, ' ')) ne '');
            defcd = trim(left(scan(defcodes, scan_i, ' '))) ;
            if scan_i > 1 then defcd_commas = trim(left(defcd_commas)) ||',' ;
            defcd_commas = trimn(defcd_commas) ||'"'|| trimn(defcd) ||'"' ;
            scan_i + 1 ;
        end;
        call symput('defcd_commas', trim(left(defcd_commas))) ;


        ** Scan through the disallowed booking rates **;
        disrates = trim(left("&disallowed_bkng_rates"));
        scan_i = 1;
        disrt_commas = '';
        do while (compress(scan(disrates, scan_i, ',')) ne '');
            disrt = trim(left(scan(disrates, scan_i, ','))) ;
            if scan_i > 1 then disrt_commas = trim(left(disrt_commas)) ||',' ;
            disrt_commas = trimn(disrt_commas) || trimn(disrt) ;
            scan_i + 1 ;
        end;
        call symput('disrt_commas', trim(left(disrt_commas))) ;
        call symput('number_of_disallowed_rates', put(scan_i - 1, 3.)) ;

 
        ** Scan through the rpo rule set ids **;
        rpo_rule_ids = trimn("&rpo_pr_rule_set_ids");
        scan_i = 1;
        rulecd_commas = '';
        do while (compress(scan(rpo_rule_ids, scan_i, ',')) ne '');
            rulecd = trim(left(scan(rpo_rule_ids, scan_i, ','))) ;
            if scan_i > 1 then rulecd_commas = trim(left(rulecd_commas)) ||',' ;
            rulecd_commas = trimn(rulecd_commas) || trimn(rulecd) ;
            scan_i + 1 ;
        end;
        call symput('rpo_rule_cds', trim(left(rulecd_commas))) ;

        ** Scan through the default rate indicator values for reporting **;
        percvals = trim(left("&cap_percentiles"));
        scan_i = 1;
        defcd_commas = '';
        do while (compress(scan(percvals, scan_i, ' ')) ne '');
            percval = trim(left(scan(percvals, scan_i, ' '))) ;
            if scan_i = 1 then call symput('low_percentile', trimn(percval));
            else if scan_i = 2 then call symput('high_percentile', trimn(percval));
            scan_i + 1 ;
        end;
        call symput('num_percentiles', trimn(put(scan_i -1, 3.))) ;

    run;
    
   
    %if &num_past_years > 2 %then %do;
        %let errflg = 1;
        %let errmsg = Too many Forecast Boundary Percentiles Defined. ;
        %goto macrend ;
    %end;
    %else %do;
        %if &low_percentile > &high_percentile %then %do;
            %let l_perc = &low_percentile ;
            %let low_percentile = &high_percentile ;
            %let high_percentile = &l_perc ;
        %end;
    %end;



    ** Read subset/additional property codes from text file, if any **;

    %if &subset_prop_codes_yn = Y or &subset_prop_codes_yn = A %then %do;

        filename proplist "&prop_code_input_list_path/&prop_code_input_list_name";

        %let rc = %sysfunc(fileref(proplist));
        %if &rc ne 0 %then %do;
            %put WARNING: Unable to assign libref to subset property list text file ;
            %put          All properties read from OYT_PR_RULE_LKUP will be processed. ;
        %end;


        ** Obtain the input prop_code list ** ;
        %let pclist_obs = 0;

        data prop_code_list ;
            infile proplist ;
            input prop_code $1-5 ;
            if _n_ = 1 then call symput('pclist_obs', 1);
        run;

        %if &pclist_obs = 0 %then %do;
            %put WARNING: Unable to assign libref to subset property list text file    ;
            %put          All properties read from OYT_PR_RULE_LKUP will be processed. ;
        %end;

        proc sort data=prop_code_list ;
            by prop_code ;
        run;

    %end;
    %else %let pclist_obs = 0;



    ** Obtain the properties to process from rpo data for 0 to Kdate **;
 
    proc sql;

        create table rpo_prop_list
        as select 
            PRL_PROP_CODE as prop_code, 
            PT_TZR as tzr 
        from       db2lref.OYT_PR_RULE_LKUP 
        inner join db2lref.OYT_PROPERTY 
        on    PRL_PROP_CODE = PT_PROP_CODE 
        where PRL_RULE_SET_ID in (&rpo_rule_cds) 
        %if &tzr <= 3 %then 
          and pt_tzr = "&tzr" ;
        order by prop_code;

    quit;



    %if &subset_prop_codes_yn = A %then %do;

         data rpo_prop_list ;
             merge rpo_prop_list 
                   prop_code_list ;           
             by prop_code ;
        run;


        ** Add tzr for the additional properties **;

        proc sql;

            create table rpo_prop_list
            as select 
                rpl.PROP_CODE, 
                pt.PT_TZR as tzr 
            from rpo_prop_list rpl, 
                 db2lref.OYT_PROPERTY pt 
            where rpl.PROP_CODE = pt.PT_PROP_CODE 
            %if &tzr <= 3 %then 
              and pt_tzr = "&tzr" ;
            order by prop_code;

        quit;

    %end;

    %dataobs(rpo_prop_list);
    %let num_rpo_rule_prop_codes = &dataobs;
    %if &num_rpo_rule_prop_codes = 0 %then %do;
        %let errflg = -1;
        %let errmsg = No properties defined for daily processing ;
        %let errmsg = &errmsg No properties for this date will be loaded ;
        %let errmsg = &errmsg in oyt_pr_apr_adjust with paa_adj_type of &rpo_adj_type ;
        %goto macrend;
    %end;



    ** Remove exception properties and, if necessary, all properties not found **;
    ** in the property subset list file, from the rpo properties process list. **;

    %if &num_exception_pcs > 0 or &subset_prop_codes_yn = Y %then %do;

        proc sql;
            create table rpo_prop_list_0
            as select rp.prop_code,
                      rp.tzr
            from rpo_prop_list rp

            %if &subset_prop_codes_yn = Y %then
            inner join prop_code_list pcl
            on rp.prop_code = pcl.prop_code ;

            %if &num_exception_pcs > 0 %then
            left join exception_properties ex
            on rp.prop_code = ex.prop_code ;
            
            %if &num_exception_pcs > 0 %then
            where compress(ex.prop_code) = '' ;          

            ;
        quit;

    %end;
    %else %do;

         data rpo_prop_list_0;
             set rpo_prop_list ;
         run;

    %end;



    ** Call the program to read RPO extended shops and create data **;
    ** suitable for loading into oyt_pr_apr_adjust.                **;

    %read_rpo_extended_shops (
            rpo_prop_list,
            oralref,
            &orausr,
            &orapw,
            &orapath,
            db2lref,
            &last_dt9,
            &actual_kdate,
            %nrquote(&rpo_rule_cds),
            &rpo_def_adj_value,
            &rpo_adj_type,
            &rpo_proc_id,
            &fill_adj_type,
            &fill_proc_id,
            rpo_apr_adjust,
            &gpo_vat_col
            );

    %if &errflg = 1 %then %goto macrend ;



    ** Find if today is the nominated day of the week for which regression **;
    ** forecast should run.                                                **;

    data null_;
        downum = weekday(today());
        daystr = substr(compress(upcase(put(today(), downame.))),1,3);
        if daystr = substr(compress(upcase("&regfcst_dow_run")),1,3) then run_rf_yn = "Y";
        else run_rf_yn = "N";
        call symput('run_regfcst_yn', compress(run_rf_yn));
    run;



    ** If the current day of the week is the one selected for running **;
    ** regression and forecast, run the relevant code section.        **;

    %if &run_regfcst_yn = Y %then %do;

        %if &as_of_date eq TODAY or &as_of_date eq MARSHA %then %do;

            ** Only if the as_of date is one of the two values above **;
            ** should the most recent plan file be found and used.   **;
 
            ** Obtain a list of filenames in the revenue plan file location **;

            filename rpdir pipe "ls -l &rp_file_location";

            data filelst;
                infile rpdir lrecl=80 pad;
                input rec $1-80;
            run;


            data pj_ext_files;
                set filelst ;
                pj_extract = trimn("&rp_file_name_prefix");
                if index(rec, pj_extract) > 0 ;
                pfloc = index(rec, pj_extract) ;
                prefxlen = length(pj_extract);

                plan_fl_dtin = substr(rec, pfloc + prefxlen, 8);
                plan_file_date = input(substr(rec, pfloc + prefxlen, 8), mmddyy8.);

                plan_file_name = substr(rec, pfloc, prefxlen +13);

                format plan_file_date date9.;
            run;

            proc sort data =  pj_ext_files ;
                by descending plan_file_date ;
            run;


            data _null_ ;
                set pj_ext_files;
                if _n_ = 1 then do;
                    call symput('plan_file_name', trimn(plan_file_name));
                end;
                else stop;
            run;

            %put selected plan file name = &plan_file_name ;


            ** Assign a fileref to the input revenue plan, including a relevant date **;
    
            filename rpfref "&rp_file_location/&plan_file_name";
        %end;
        %else 
            filename rpfref "&rp_file_location/&rp_file_name_prefix.&last_mdy..txt";
        ;


        %let rc = %sysfunc(fileref(rpfref));
        %if &rc ne 0 %then %do;
            %let errflg = 1;
            %let errmsg = WARNING: Unable to assign fileref to the revenue plan text file &rp_file_location/&rp_file_name_prefix._&last_mdy;
            %goto macrend;
        %end;




        ** Find the list of prop codes to process through regression / forecast. **;

        proc sort data = db2lref.OYT_PR_RULE_LKUP 
                   out = prop_list (rename= (prl_prop_code = prop_code));
            where prl_rule_set_id = &pr_rule_set_id;
            by prl_prop_code;
        run;

        %dataobs(prop_list);
        %let num_rule_prop_codes = &dataobs;
        
        %let num_process_pcs =  %eval(&num_rule_prop_codes + &pclist_obs) ;

        %if (&subset_prop_codes_yn = Y or &subset_prop_codes_yn = A) 
            and &num_process_pcs > 0 %then %do;

            %if &pclist_obs > 0 %then %do;

                data prop_list ;

                    %if &num_rule_prop_codes > 0 %then 
                          merge prop_list (in=lkup_) ;
                    %else set ;
                          prop_code_list (in=pcfile_) ;
                    by prop_code;
                    
                    %if &num_rule_prop_codes > 0 %then %do;
                        if lkup_
                        %if &subset_prop_codes_yn ne A %then 
                            and ;
                        %else or ;
                        pcfile_ ;
                    %end;
                run;

            %end;
        %end;


        ** Add the exception property list **;

        %if &num_exception_pcs > 0 %then %do;


            data prop_list;
                %if &num_process_pcs > 0 %then
                merge prop_list ;
                %else set ;
                      exception_properties ;
                by prop_code;
            run;

        %end;
    
        %let num_process_pcs = %eval(&num_exception_pcs + &num_process_pcs);



        %if &num_process_pcs > 0 %then %do ;

            ** Subset the prop_list data set for the required timezone **;
            ** - or at least add the assigned currency.                **;
        
            proc sql;

                create table pclist_0 
                as select pl.prop_code,
                          oy.pt_currency_code as mi_currency, 
                          oy.pt_tzr as paa_tzr
                from prop_list pl ,
                db2lref.oyt_property oy
                where pl.prop_code = oy.pt_prop_code
                %if &tz <=3 %then        
                  and oy.pt_tzr = "&tzr" ;
                order by pl.prop_code
                ;
            quit ;

        %end;


        ** Copy pclist_0 to the sasdmart directory **;
        data sasdmart.pclist_0 ;
            set pclist_0;
        run;
 

        ** Write prop_codes to lists of 1000 or so properties **;

        %let loop = 1;
        %let num_pc_lists = 1;
        %let dataobs =1;

        %do %while (&dataobs > 0);

            %let lastiter = %eval(&loop -1);

            data pclist_&loop ;
                length pcstr $10000;
                set pclist_&lastiter end=eof;

                retain pcstr ;

                if _n_ > 1 and _n_ <= 1000 then pcstr = trimn(pcstr) ||"," ;
    
                if _n_ <= 1000 then pcstr = trimn(pcstr) || compress("'"|| prop_code ||"'");
                else output ;

                if _n_ = 1000 or eof then do;
                    call symput(compress("pclist_&loop"), trimn(pcstr)) ;
                    if eof then call symput('num_pc_lists', put(&loop, 4.));
                end; 

                drop pcstr ; 
            run;

            %put &&pclist_&loop ;

            %dataobs(pclist_&loop) ;
            %let loop = %eval(&loop + 1);


        %end;

        %put num_pc_lists = &num_pc_lists;
        

        %if %upcase(&run_reg_yn) = Y and &num_process_pcs > 0 %then %do ;

            ** Ensure that an older version of the permanent fs_retail_rate **;
            ** data set is not inadvertantly used by this process.          **;

            %if %sysfunc(exist(sasdmart.fs_retail_rate)) > 0 %then %do ;

                proc datasets lib=sasdmart nolist;
                    delete fs_retail_rate ;
                quit;

            %end;


            ** Call sub-program to read Netezza rate information **;

            %download_retail_rt (netlref,
                               oralref,
                               &first_dt9,
                               &last_dt9,
                               prop_list,
                               &number_of_seg_vals,
                               %nrquote(&segval_commas),
                               &ab_tier_los_txt_val,
                               &number_of_disallowed_rates,
                               %nrbquote(&disrt_commas),
                               &rate_outlier_remove_multiplier,
                               sasdmart.fs_retail_rate);

            %if &errflg > 0 %then %goto macrend;


           %if %sysfunc(exist(sasdmart.pj_hist_smry)) > 0 %then %do;

               proc datasets lib = sasdmart nolist ;
                   delete pj_hist_smry ;
               quit;

           %end;


            ** Call sub-program to read DB2 apr rate information **;

            %download_apr(db2lref,
                         &first_dt9,
                         &last_dt9,
                         prop_list,
                         sasdmart.pj_hist_smry);

            %if &errflg > 0 %then %goto macrend;



            ** Join the results of the calling the previous two programs **;

            data apr_rt_hist
                 zero_adr_apr;
                merge sasdmart.pj_hist_smry (rename = (inv_date = stay_dt)) 
                      sasdmart.fs_retail_rate (keep = prop_code stay_dt adr);
                by prop_code stay_dt;

                dow = weekday(stay_dt);
                mon = month(stay_dt);
 
                if &indep_var > 0 and adr > 0 then output apr_rt_hist  ;
                else output zero_adr_apr;
        
            run;



            ** Add MI Currency from the rpo_property table **;

            data apr_rt_hist ;
                merge apr_rt_hist (in=apr_) 
                      pclist_0 ;
                by prop_code ;
                if apr_ ;
            run;

 

            ** Convert the independant variable value currency, if **;
            ** it differs from that in oyt_property.               **;

            %convert_currency(apr_rt_hist,
                              db2mmplf,
                              &indep_var,
                              stay_dt,
                              %nrquote(prop_code, apr, adr, dow, mon),
                              apr_rt_hist);



            ** Call the regression program **;

            %reg_adr(apr_rt_hist,
                     prop_list,
                     parameter_estimates,
                     &first_dt9,
                     &last_dt9,
                     &outlier_filter_sd_mult,
                     model_var = &model_var,
                     indep_var = &indep_var)

            %if &errflg > 0 %then %goto macrend;



            ** Filter the returned parameter estimates for any properties that for any reason **;
            ** do not have regression estimate results.                                       **;

            data parameter_estimates 
                 filtered_failed_reg_props (keep=prop_code reg_failed_yn filtered_yn);
                set parameter_estimates ;
                if reg_failed_yn = 'Y' or filtered_yn = 'Y' then output filtered_failed_reg_props ;
                else output parameter_estimates;
            run;


            %dataobs(filtered_failed_reg_props) ;
            %let num_reg_failed_props = &dataobs;
            %if &num_reg_failed_props > 0 %then %do;

                data _null_;
                    length prop_list $10000;

                    set filtered_failed_reg_props end = eof;
                    retain prop_list ;

                    if _n_ = 1 then prop_list = compress(prop_code) ;
                    else prop_list = compress(prop_list ||','|| prop_code ) ;

                    if eof then do;
                        call symput ('reg_failed_prop_list', compress(prop_list));
                    end;
                run;

            %end;
            %else %let reg_failed_prop_list = ;

            %put Reg failed Prop List: &reg_failed_prop_list;


            ** Add the parameter estimates to the permanent data set. **;

            %dataobs(sasdmart.parameter_estimates) ;
        
            %if &dataobs > 0 %then %do;

                 data sasdmart.parameter_estimates ;
                     merge sasdmart.parameter_estimates 
                           parameter_estimates;
                     by asofdate prop_code indepvar;
                 run; 
            %end;
            %else %do;

                 proc sort data = parameter_estimates
                             out = sasdmart.parameter_estimates ;
                     by asofdate prop_code indepvar;
                 run; 
            %end;
 
        %end; /* End of Regression data prep and regression process */
        %else %let num_reg_failed_props = 0 ;    



        ** Define K-date for the as-of date in use **;
    
        %if &rpo_kdate_yn = N  %then %do;

            data _null_ ;
                asofdt = "&last_dt9"d ;

                * K-date is the Sunday before the end of the year increment *;
                * from the as-of date.                                      *;
                yrincdt = intnx('year', "&last_dt9"d, 1, 'SAME');
                dow_yrinc = weekday(yrincdt);
                kdate = yrincdt - dow_yrinc ;
         
                call symput ('kdate', put(kdate+1, date9.));
            run;
        %end;


        %if %sysfunc(exist(sasdmart.revenue_plan_data)) > 0 %then %do;

             proc datasets nolist lib = sasdmart;
                 delete revenue_plan_data ;
             quit;

        %end;

    
        ** Read the Revenue Plan Data **;

        %read_revenue_plan (rpfref,
                    prop_list,
                    sasdmart.revenue_plan_data,
                    &first_dt9,
                    &last_dt9,
                    &kdate,
                    &max_days_left,
                    &adr_default_val,
                    &apr_default_val                
                    );


        %if &errflg > 0 %then %goto macrend;


        proc sql ;

            create table default_rp_apr_props
            as select distinct prop_code
            from sasdmart.revenue_plan_data
            where default_apr_rate in (&defcd_commas);

        quit;

        %dataobs (default_rp_apr_props);
        %let num_def_rp_rate_props = &dataobs ;


        %if &num_def_rp_rate_props > 0 %then %do;

            data _null_;
                length prop_list $10000;

                set default_rp_apr_props end = eof;
                retain prop_list ;

                if _n_ = 1 then prop_list = compress(prop_code) ;
                else prop_list = compress(prop_list ||','|| prop_code) ;

                if eof then do;
                    call symput ('default_rp_apr_prop_list', compress(prop_list));
                end;
            run;
     
        %end;



        %if %sysfunc(exist(sasdmart.&output_data_set_name)) > 0 %then %do ;
        
             proc datasets nolist lib=sasdmart;
                 delete &output_data_set_name ;
             quit;
        
        %end;



        **  Send revenue plan actual values to forecast program **;  
        %if default_yearly_pct_increase = %str() %then %let default_yearly_pct_increase = 0;

        %forecast_adr (sasdmart.revenue_plan_data,
                sasdmart.fs_retail_rate,
                sasdmart.parameter_estimates,
                &num_exception_pcs,
                exception_properties,
                forecast_table,
                db2lref,
                &first_dt9,
                &last_dt9,
                &max_days_left,
                &kdate,
                &default_yearly_pct_increase,
                &low_percentile,
                &high_percentile,
                apr_staydt_col = inv_date,
                adr_var = adr,
                indep_var = &indep_var           
                );


        %if &errflg > 0 %then %goto macrend;



        ** Format the returned data set correctly **;

        data forecast_table2;
         
            merge pclist_0 (keep = prop_code paa_tzr)
                  forecast_table (in=fcst_);
            
            by prop_code;
            if fcst_ ;

            PAA_ADJ_TYPE = compress("&paa_adj_type") ; 

            if (fcst + apr = .) or apr = 0 then PAA_ADJ_VALUE = &default_adj_out_value ;
            else PAA_ADJ_VALUE = round (fcst / apr, 0.00001) ;
            PAA_PROC_ID = "&paa_proc_id" ;
            PAA_LAST_UPD_TS = datetime() ;

            %if &tzr <= 3 %then paa_tzr = "&tzr" %str(;) ;

            rename prop_code = paa_prop_code
                   stay_dt = paa_stay_date ;

            keep prop_code stay_dt
                 paa_tzr paa_adj_type paa_adj_value paa_proc_id paa_last_upd_ts ;

        run;

  


        ** Add the 'A' forecast values to 'R' records received from the **;
        ** read_rpo_extended_shops macro.                               **;

        %dataobs(rpo_apr_adjust) ;

        data sasdmart.&output_data_set_name ;
            set %if &dataobs > 0 %then
                rpo_apr_adjust ;
                forecast_table2 
            ;
        run;

    %end;
    %else %do;

        proc copy in = work out= sasdmart mt=data ;
            select rpo_apr_adjust ;
        run;

        proc datasets lib = sasdmart nolist;
            delete &output_data_set_name ;
            change rpo_apr_adjust = &output_data_set_name;
        quit;
  
    %end;
    
 


    ** Write out the forecast values to the output OYT_PR_APR_ADJUST table **;
    %write_forecast_to_db(sasdmart.&output_data_set_name,
                          &num_exception_pcs,
                          exception_properties,
                          &tzr,
                          &first_dt9,
                          &last_dt9,
                          &kdate,
                          &max_days_left,
                          &indep_var,
                          &paa_adj_type,
                          &paa_proc_id,
                          &default_adj_out_value,
                          db2lrout,
                          &db2db,
                          &db2usr,
                          &db2pw,
                          &db2schema
                          ) ;
 

    %macrend:


    ** Report on properties that failed regression, for any reason, or used default **;
    ** revenue plan rate values.                                                    **;
    
    %let num_prob_props = %eval(&num_reg_failed_props + &num_def_rp_rate_props) ;

    %if &num_emails > 0 and (&errflg ne 0 or &num_prob_props > 0) %then %do;

       
        %if &num_prob_props > 0 %then %do;
       
            ** Create a file that contains the information on filtered / filled files **;
       
            filename repfile "&report_file_loc/EGP_filter_filled_pcs.txt";
       
            data _null_ ;
                length subrec $132 ;
                file repfile lrecl=132;
                %if &num_reg_failed_props > 0 %then %do;
                     put 'Properties Failing Regression: ';
                     fail_props = trimn("&reg_failed_prop_list") ;
                     len_rfpl = length(trimn(fail_props)) ; 
                     subrec = substr(trimn(fail_props),1,132);
                     i = 1;
                     do while(compress(subrec) ne '') ;
                         put subrec ; 
                         i +1 ;
                         if len_rfpl > ((i+1) * 132) then
                          subrec = substr(trimn(fail_props), (i*132 +1), 132);
                         else
                          subrec = substr(trimn(fail_props), (i*132 +1));
                     end;
       
                %end;
       
                %if &num_def_rp_rate_props > 0 %then %do;
                     put "Properties With Filled-In Rev Plan &indep_var values: ";
                     fill_props = trimn("&default_rp_apr_prop_list");
                     len_rfpl = length(fill_props) ; 
                     subrec = substr(trimn(fill_props),1,132);
                     i = 1;
                     do while(compress(subrec) ne '') ;
                         put subrec ; 
                         i +1 ;
                         if len_rfpl > ((i+1) * 132) then
                          subrec = substr(trimn(fill_props), (i*132 +1), 132);
                         else
                          subrec = substr(trimn(fill_props), (i*132 +1));
                     end;
        
                %end;
            run;
        %end;
       
       
        data _null_ ;
            length msgcmd $10000;
            * Send an e-mail including the text report as an attachment. *;
            * if any problem properties were found.                      *;
            
            %if &num_prob_props > 0 %then 
             msgcmd = 'uuencode '|| "&report_file_loc/EGP_filter_filled_pcs.txt" ||
                         " EGP_filter_filled_pcs.txt" %str(;);
            %else 
             msgcmd = 'echo "' || "&errmsg" ||'"' %str(;) ;
       
            msgcmd = trimn(msgcmd) ||' | mail -s " EGP Regression Process, '|| "&last_dt9" ||'" "';
       
            do i = 1 to &num_emails ;
                if i > 1 then msgcmd = trim(left(msgcmd)) ||', ';  
                msgcmd = trim(left(msgcmd)) ||' '|| compress(symget(compress('email_'|| put(i,3.))));  
            end;
            msgcmd = trimn(msgcmd) ||'"' ;
           
            call sysexec(msgcmd);
       
        run;
     
    %end;



    libname db2lref ;
    libname db2mmplf ;
    libname oralref ;
    libname netlref ;


    %if &errflg > 0 %then %do ;

        %put ;
        %put PROGRAM ENDING: &errmsg ;
        %put ;

        data _null_ ;
           * abort return ;
        run;


    %end;


%mend;


/*
%let tz = %sysget(TZR) ;
%let top_dir = %sysget(SAS_TOP_DIR);
%let config_file_name = %sysget(CONFIG_FILE);
*/


%let tz = 4 ;
%let top_dir =/fcst2/ah/apr;
%let config_file_name = egp_config.txt;


options mprint symbolgen mlogic
        sasautos = ('sasautos', "&top_dir/code");



%let RemName1=OYS5 7551;
options comamid=TCP;
* CHECK THE PASSWORD BEFORE RUNNING ;
signon RemName1 user=sasdemo password=Own_Aug14 NOCSCRIPT;


rsubmit ;

options source mprint symbolgen;
      
libname db2oys5  DB2 db=DSNP
                   user=oyapnet 
               password=oyapnet2
                 schema=YMP ;

libname db2mpoy5  DB2 db=DSNP
                    user=oyapnet 
                password=oyapnet2
                  schema=MMP ;

libname oraoys5 oracle user=rpobatchuser password="RPOBATCHUSER4PROD" path=MIRPOPRD ;


endrsubmit;

%run_egp_process (&top_dir/lib, &config_file_name, &tz) ;

signoff;


