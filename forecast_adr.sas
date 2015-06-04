******************************************************************;
** Program: forcast_adr.sas                                     **;
**                                                              **;
** Purpose: Obtain forecast coefficients from the data mart     **;
**          parameter estimates data set, and calculte forecast **;
**          [modelvar] values from those and the input historic **;
**          [indepvar] values.                                  **;
**                                                              **;
** Pre-reqs: Libref pointed to by db2lib parameter should       **;
**           alread be assigned. All properties for which data  **;
**           should be read should be listed in a number of     **;
**           of 'pclist_' macro variables assigned by the      **;
**           the calling program.                               **;
**                                                              **;
** By:       Yunhong Liao, Andrew Hamilton, June 2014.          **;
**                                                              **;
******************************************************************;

%macro forecast_adr (inaprdsn,
                inadrdsn,
                paramestdsn,
                num_exception_props,
                exception_properties_ds,           
                outdsn,
                db2lib,
                startdt9,
                lastdt9,
                max_days_out,
                kdate,
                def_yrly_increase,
                low_perc,
                high_perc,
                apr_staydt_col = inv_date,
                adr_var = adr,
                indep_var = apr
                );




    ** Obtain regression coefficients to use ;

    data parameter_estimates_before
         parameter_estimates_after ;
  
        set &paramestdsn ; 

        where prop_code in (
        %do j = 1 %to &num_pc_lists;
            %if &j > 1 %then , ;
            &&pclist_&j 
        %end;
        )
        and compress(upcase(indepvar)) = compress(upcase("&indep_var"))
        ;

 
        by asofdate  ;
        
        if asofdate <= "&lastdt9"d then output parameter_estimates_before;
        else output parameter_estimates_after ;
            
    run;
        
        
        
    proc sort data = parameter_estimates_before ;
        by prop_code descending asofdate;
    run;
    
    
    
    ** Select the set of parameter estimates that are closest to the asofdate **;
    
    data parameter_estimates ;
        set parameter_estimates_before ;
        by prop_code descending asofdate ;
        if first.prop_code;

        exception_yn = 'N';
    run;


    %dataobs(parameter_estimates);
    %if &dataobs = 0 %then %do;

        ** If no parameter estimates are available from before or on the as of date, **;
        ** look for later parameter estimates.                                       **;

        %dataobs (parameter_estimates_after);
        %if &dataobs > 0 %then %do ;

            proc sort data = parameter_estimates_after ;
                by prop_code asofdate;
            run;


            data parameter_estimates ;
                set parameter_estimates_after ;
                by prop_code asofdate ;
                if first.prop_code  ;
            run;

            %let errflg = 2;
            %let errmsg = Program WARNING: No parameter estimates were found before ;
            %let errmsg = &errmsg the as of date of &predict_date_start. Parameter  ;
            %let errmsg = &errmsg estimates from after the as of date will be used  ;

        %end;
        %else %do;

            %let errflg = 1;
            %let errmsg = Program ERROR: No parameter estimates were found before ;
            %let errmsg = &errmsg or after the as of date of &predict_date_start. ;

        %end;
    %end;


    ** Sort the exception properties data set for joining with the parameter **;
    ** estimates data set.                                                   **;

    %if &num_exception_props > 0 %then %do;

        proc sort data = &exception_properties_ds ;
            by prop_code ;
        run;

        data parameter_estimates ;
            merge parameter_estimates (in=parm_)
                  &exception_properties_ds (in=exc_) ;
            by prop_code ;
            if parm_ ;
            if exc_ then exception_yn = 'Y';
        run;

    %end;



    ** Obtain por_yrly_increase from the OYT_PR_OTHR_RULES table **;

    data othr_rules_yrly_increase;
        set &db2lib..oyt_pr_othr_rules ;
        where por_prop_code in(&pclist_1 
            %do j = 2 %to &num_pc_lists ;
                , &&pclist_&j
            %end;
              );

        if por_yrly_increase ne . then yrly_increase = por_yrly_increase ;
        else yrly_increase = &def_yrly_increase ;
        oneyr_fctr = 1 + (yrly_increase/100) ;
        twoyr_fctr = (1 + (yrly_increase/100))**2 ;
            
        rename por_prop_code = prop_code ;

        keep por_prop_code yrly_increase oneyr_fctr twoyr_fctr ;

    run; 


    proc sort data =  othr_rules_yrly_increase ;
        by prop_code ;
    run;



    ** Obtain regression coefficients for all necessary dates. **;

    data coeff1;
        merge parameter_estimates (in=param_)
              othr_rules_yrly_increase (in=othrl_);
        by prop_code ;

        if param_ ;

        if not othrl_ then do ;
            oneyr_fctr = 1 + (&def_yrly_increase/100);
            twoyr_fctr = (1 + (&def_yrly_increase/100))**2 ;
        end;

        format stay_dt mmddyy10.;

        array a1(6) dow2-dow7;
        array a2(11) mon2-mon12;

        if exception_yn = 'N' then 
         do i = "&Kdate"d to ("&lastdt9"d + &max_days_out) ;
            stay_dt = i;

            link monthdow ;

            output;
        end;

        else
         do i = "&lastdt9"d to ("&lastdt9"d + &max_days_out) ;
            stay_dt = i;

            link monthdow ;

            output;
        end;


        format stay_dt date9. ;
        drop i j;

        return;

        monthdow:

            year = year(stay_dt) ;
            month = month(stay_dt) ;
            dow = weekday(stay_dt);
      
            if stay_dt < "&kdate"d then fctr = oneyr_fctr ;
            else fctr = twoyr_fctr ;

            do j=1 to 6;
                a1(j)=(dow=j+1);
            end;
            do j=1 to 11;
                a2(j)=(month=j+1);
            end;

        return;

    run;



    ** Add projected apr as fcst **;

    data fcst2;
        merge coeff1 (in = in1_) 
              &inaprdsn 
              %if &apr_staydt_col ne stay_dt  %then %do;
                     (rename =(&apr_staydt_col = stay_dt )) 
              %end; 
        ;
        by prop_code stay_dt;
        if in1_;

        fcst =   (intercept  * fctr)
               + (coalesce(dow2_coef,0) * dow2 * fctr)
               + (coalesce(dow3_coef,0) * dow3 * fctr)
               + (coalesce(dow4_coef,0) * dow4 * fctr)
               + (coalesce(dow5_coef,0) * dow5 * fctr)
               + (coalesce(dow6_coef,0) * dow6 * fctr)
               + (coalesce(dow7_coef,0) * dow7 * fctr)
               + (coalesce(mon2_coef,0) * mon2 * fctr)
               + (coalesce(mon3_coef,0) * mon3 * fctr)
               + (coalesce(mon4_coef,0) * mon4 * fctr)
               + (coalesce(mon5_coef,0) * mon5 * fctr)
               + (coalesce(mon6_coef,0) * mon6 * fctr)
               + (coalesce(mon7_coef,0) * mon7 * fctr)
               + (coalesce(mon8_coef,0) * mon8 * fctr)
               + (coalesce(mon9_coef,0) * mon9 * fctr)
               + (coalesce(mon10_coef,0) * mon10 * fctr)
               + (coalesce(mon11_coef,0) * mon11 * fctr)
               + (coalesce(mon12_coef,0) * mon12 * fctr)
               + (coalesce(apr_coef,0) * &indep_var);

        keep prop_code year month stay_dt fcst &indep_var 
             apr fctr;
        ;
    run;


    proc sort data=fcst2;
        by prop_code month stay_dt;
    run;



    ** Remove bad data before measuring err **;

    proc summary data=&inadrdsn noprint ;
        class prop_code month ;
        var &adr_var ;
        output out=adr_percentiles (where = (_type_ in (2,3)))
                   p&low_perc = adr_p10 p&high_perc = adr_p90 ;
    quit;




    ** Merge the forecast values with the data set holding 10th and 90th percentile **;
    ** values of the input 'adr' data set.                                          **;

    data capped_forecast_values;
        length capped_yn $2;

        merge fcst2 (in=fcst_)
              adr_percentiles (in=perc_ where = (_type_ = 3)) ;
        by prop_code month;

        if fcst_ ;

        if not perc_ then capped_yn = 'NA' ; 
        if perc_ then do ;
            if fcst < (fctr * adr_p10) then do;
                fcst = (fctr * adr_p10) ;
                capped_yn = 'Y' ;
            end;
            else if fcst > (fctr * adr_p90) then do;
                fcst = (fctr * adr_p90) ;
                capped_yn = 'Y' ;
            end;
            else capped_yn = 'N' ;
        end;
       
    run;



    data forecast_table2;
        merge capped_forecast_values (in = fcst_)
              adr_percentiles (   in = perc_
                                drop = month _freq_ 
                               where = (_type_ = 2)
                              rename = (adr_p10 = pc_adr_p10
                                        adr_p90 = pc_adr_p90)) ;
        by prop_code ;
        
        if fcst_;

        if perc_ and capped_yn = 'NA' then do;
            if fcst < (fctr * pc_adr_p10) then do;
                fcst = (fctr * pc_adr_p10) ;
                capped_yn = 'M' ;
            end;
            else if fcst > (fctr * pc_adr_p90) then do;
                fcst = (fctr * pc_adr_p90) ;
                capped_yn = 'M' ;
            end;
        end;

        drop adr_p10 pc_adr_p10 adr_p90 pc_adr_p90 ;

    run;
 

    proc sort data = forecast_table2 
               out = &outdsn ;
        by prop_code stay_dt ;
    run;


    %submacrend:

%mend;
