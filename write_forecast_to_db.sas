******************************************************************;
** Program: write_forecast_to_db.sas                            **;
**                                                              **;
** Purpose: Write forecast values to the DB2 table              **;
**          OYT_PR_APR_ADJUST                                   **;
**                                                              **;
** Pre-reqs: Libref pointed to by db2lib parameter should       **;
**           alread be assigned.                                **;
**                                                              **;
** By:       Andrew Hamilton, June 2014.                        **;
**                                                              **;
******************************************************************;

%macro write_forecast_to_db (infcstdsn,
                          num_exc_pcs,
                          excep_props_ds,
                          tz,
                          startdt9,
                          lastdt9,
                          kdate9,
                          max_days_out,
                          indepvar,
                          paa_adj_type,
                          paa_proc_id,
                          def_out_val,
                          db2lib,
                          db2db,
                          db2usr,
                          db2pwd,
                          db2schema
                          ) ;



    %let errflg = 0;


    ** Convert kdate in date9 format to correct format **;
    data _null_;
        kdate = "&kdate9"d;
        year = put(year(kdate),4.);
        month = put(month(kdate), z2.);
        day = put(day(kdate),z2.);
        call symput('db2kdate', compress("'"|| year ||'-'|| month ||'-'|| day ||"'"));
        call symput('maxdate', put("&lastdt9"d + &max_days_out, date9.));
    run;



    ** Find combinations of being-dealt-with prop_codes and staydates **;
    ** that exist in the target table.                                **;

    ** Obtain all property codes represented in the update data. **;

    proc sql;

        create table distinct_update_pcs
        as select distinct paa_prop_code 
        from &infcstdsn ;

        select compress("'"|| paa_prop_code ||"'") 
        into: pc_updt_list 
        separated by ","
        from distinct_update_pcs ;

    quit;


    data existing_combos ;
        set &db2lib..oyt_pr_apr_adjust ;
        where paa_prop_code in (&pc_updt_list)
        and paa_stay_date between "&lastdt9"d and "&maxdate"d ;
        rename paa_prop_code = prop_code
               paa_stay_date = stay_date ;
    run;


   %dataobs (existing_combos) ;

   %if &dataobs > 0 %then %do;

        ** Remove any old values from the temp table **;

        proc sql;

            connect to db2 (   db = &db2db 
                             user = &db2usr 
                            using = &db2pw 
                            );

            execute (delete from &db2schema..oyt_sp_apr_adjwrk) by db2 ;

            disconnect from db2;



            ** Insert the actual existing combos in the temp table **;

            insert into &db2lib..OYT_SP_APR_ADJWRK
            (PAA_TZR, 
             PAA_PROP_CODE, 
             PAA_STAY_DATE,
             PAA_ADJ_TYPE, 
             PAA_ADJ_VALUE, 
             PAA_PROC_ID,
             PAA_LAST_UPD_TS)
            select f.PAA_TZR, 
                   f.PAA_PROP_CODE, 
                   f.PAA_STAY_DATE,
                   f.PAA_ADJ_TYPE, 
                   f.PAA_ADJ_VALUE, 
                   f.PAA_PROC_ID,
                   f.PAA_LAST_UPD_TS 
            from &infcstdsn f,
                 existing_combos e
            where f.paa_prop_code = e.prop_code
              and f.paa_stay_date = e.stay_date
            ;
        
        quit;



        ** Insert data from the temp table into the main table table **;

        proc sql;

            connect to db2 (   db = &db2db 
                             user = &db2usr 
                            using = &db2pw 
                           );
        
            execute(update &db2schema..oyt_pr_apr_adjust a
                    set (a.paa_adj_value, 
                         a.paa_proc_id,
                         a.paa_adj_type,
                         a.paa_last_upd_ts) =
                    (select b.paa_adj_value, 
                            b.paa_proc_id,
                            b.paa_adj_type,                    
                            b.paa_last_upd_ts
                     from &db2schema..oyt_sp_apr_adjwrk b
                     where a.paa_prop_code = b.paa_prop_code
                       and a.paa_stay_date = b.paa_stay_date
                       and a.paa_tzr = b.paa_tzr
                     ) 
                     where exists (select 1 from &db2schema..oyt_sp_apr_adjwrk c
                                   where a.paa_prop_code = c.paa_prop_code
                                     and a.paa_stay_date = c.paa_stay_date
                                     and a.paa_tzr       = c.paa_tzr)
                     ) 

            by db2;
           

            execute (commit) by db2;

            disconnect from db2;

        quit;


        %if &sqlrc >= 4 %then %do;

            %let errflg = 1;
            %let errmsg = Unable to execute update command against oyt_pr_apr_adjust ;
            %goto submacrend ;

        %end;
    %end;



    ** Clear out the temp table and refill it with new insert records **;

    proc sql;

        connect to db2 (   db = &db2db 
                         user = &db2usr 
                        using = &db2pw 
                        );

        execute (delete from &db2schema..oyt_sp_apr_adjwrk) by db2 ;

        disconnect from db2;

    quit;



    ** Insert the prop_code, stay_date combos that were not found **;
    ** in the output oyt_pr_apr_adjust table in the temp table.   **;

    proc sql ;

        insert into &db2lib..OYT_SP_APR_ADJWRK
        (PAA_TZR, 
         PAA_PROP_CODE, 
         PAA_STAY_DATE,
         PAA_ADJ_TYPE, 
         PAA_ADJ_VALUE, 
         PAA_PROC_ID,
         PAA_LAST_UPD_TS)
        select f.PAA_TZR, 
               f.PAA_PROP_CODE, 
               f.PAA_STAY_DATE,
               f.PAA_ADJ_TYPE, 
               f.PAA_ADJ_VALUE, 
               f.PAA_PROC_ID,
               f.PAA_LAST_UPD_TS 
        from &infcstdsn f
        left join existing_combos e
        on  f.paa_prop_code = e.prop_code
        and f.paa_stay_date = e.stay_date
        where compress(e.prop_code) = ''
        ;
    
    quit;


    ** Add the new temp table records to the output table **;

    proc sql;

        connect to db2 (   db = &db2db 
                         user = &db2usr 
                        using = "&db2pw" 
                        );
    
        execute (insert into &db2schema..oyt_pr_apr_adjust 
                 select * from &db2schema..OYT_SP_APR_ADJWRK) by db2;

        execute (commit) by db2;
        
        disconnect from db2;

    quit;


    %if &sqlrc >= 4 %then %do;

        %let errflg = 1 ;
        %let errmsg = Unable to insert records into DB2 table oyt_pr_apr_adjust ;

    %end; 



    %submacrend:

%mend;
