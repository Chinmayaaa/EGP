*****************************************************************;
** Program: download_retail_rt.sas                             **;
**                                                             **;
** Purpose: Download necessary data for selected properties    **;
**          from netezza table mrdw_fact_agg_dr_rev_analysis   **;
**          to calculate adr.                                  **;
**          Validate output rates with std_rmp_usr/std_rmp_def **;
**          values read from Oracle table rpo_property.        **;
**                                                             **;
** Pre-reqs: Librefs netlib and oralib should already be       **;
**          assigned. A data set of property codes that the    **;
**          rates should be read for should also already exist.**;
**                                                             **;
** By:      Yunhong Liao, Andrew Hamilton, June 2014.          **;
**                                                             **;
*****************************************************************;

%macro download_retail_rt(netlib,
                          oralib,
                          firstdt9,
                          lastdt9,
                          prop_code_ds,
                          num_segvars,
                          segvars_list,
                          ab_tier_los_txt,
                          num_dis_rates,
                          dis_rates_list,
                          outlier_remove_mult,
                          outdsn);

   
    %let errflg = 0;



    ** 2. Get Standard Room Pool and Cost Per Occupancy Room from RPO_PROPERTY **;

    proc sql;
        create table fs_std_rmp 
        as select 
            a.prop_code, 
            coalesce(a.std_rmp_usr, a.std_rmp_def) as std_rmp,
            coalesce(cpor_usr, cpor_def) as cpor
        from &oralib..rpo_property a,
             &prop_code_ds b
        where a.prop_code = b.prop_code
        order by prop_code 
        ;

    quit;


    %dataobs(fs_std_rmp);
    %if &dataobs = 0 %then %do;
         %let errflg = 1;
         %let errmsg = No records read from RPO_PROPERTY ;
         %goto submacrend ;
    %end;


    ** 1b. Obtain Currency Change Date per property - if any exists. **;
    proc sort data = &oralib..rpo_currency nodupkey 
        out = rpo_currency_chnge ;
        by prop_code;
        where curcy_code_from ne curcy_code_to ;
    run;



    ** 2. Get retail rate from netezza **;

    proc sql;

        create table rt1 
        as select 
            property_cd as prop_code,
            date_stay_dt as stay_dt,  
            market_seg_nm,
            rate_pgm_txt,
            room_pool_cd, 
            roomnights_qty,
            net_rev_local_amt
        from &netlib..mrdw_fact_agg_dr_rev_analysis
        where property_cd in (
            %do k = 1 %to &num_pc_lists ;
                &&pclist_reg_&k
                %let m = %eval(&k +1);
                %if &k < &num_pc_lists and %length(&&pclist_reg_&m) > 1 %then ,;
            %end ;
                             )
          and date_stay_dt between "&firstdt9"d and "&lastdt9"d
          %if &num_segvars > 0 %then
          and market_seg_nm in (&segvars_list) ;
          %if %length(&ab_tier_los_txt) > 0 %then
          and ab_tier_los_txt = "&ab_tier_los_txt" ;
        ;

    quit;

    %dataobs(rt1);
    %if &dataobs = 0 or &sqlrc > 4 %then %do;
         %let errflg = 1;
         %let errmsg = No records read from MRDW_FACT_AGG_DR_REV_ANALYSIS;
         %goto submacrend ;
    %end;



    proc sql;

        create table rt2 
        as select a.*,
                  a.net_rev_local_amt / roomnights_qty as book_rate,
                  b.std_rmp,
                  datepart(cc.data_dt) as curcy_chng_dt format=date9. 
        from      rt1 a

        left join fs_std_rmp b
        on  a.prop_code    = b.prop_code 
        and a.room_pool_cd = b.std_rmp

        left join rpo_currency_chnge cc
        on a.prop_code     = cc.prop_code

        where b.std_rmp ~= ''
          and (a.net_rev_local_amt / a.roomnights_qty) > b.cpor
        %if &num_dis_rates > 0 %then
         and (a.net_rev_local_amt / a.roomnights_qty) not in (&dis_rates_list) ;

         and (a.stay_dt >= datepart(cc.data_dt) or cc.data_dt = .)

        order by prop_code,
                 stay_dt        
        ;

    quit;


    %dataobs(rt2);
    %if &dataobs = 0 %then %do;
         %let errflg = 1;
         %let errmsg = No records output from join of RPO_PROPERTY and ;
         %let errmsg = &errmsg MRDW_FACT_AGG_DR_REV_ANALYSIS;
         %goto submacrend ;
    %end;


    proc summary data = rt2 nway noprint;
        class prop_code stay_dt ;
        var book_rate ;
        output out = rt2sum mean = mean_book_rate ;
    quit;


    data filtered_rt ;
        merge rt2 
              rt2sum ;
        by prop_code stay_dt ;
        
        if book_rate / mean_book_rate > &outlier_remove_mult then delete ;
    run;
     


    proc sql;

        create table &outdsn
        as select prop_code,
                  stay_dt,
                  month(stay_dt) as month,
                  count(prop_code) as cnt,   
                  sum(roomnights_qty) as total_rms,
                  sum(net_rev_local_amt) as total_rev,
                  round(sum(net_rev_local_amt) / sum(roomnights_qty), 0.01) as adr
        from filtered_rt
        group by prop_code,
                 stay_dt
        order by prop_code,
                 stay_dt
        ;

    quit;


    %submacrend:

%mend;
