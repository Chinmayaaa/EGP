******************************************************************;
** Program: reg_adr.sas                                         **;
**                                                              **;
** Purpose: Use proc reg to obtain regression coefficients from **;
**          modelling the indicated 'model_var' against the     **;
**          indicated 'indep_var', along with 11 month and 6    **;
**          dow values.                                         **;
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

%macro reg_adr (indsn,
                prop_code_ds,
                outestdsn,
                startdt9,
                lastdt9,
                sd_multiplier,
                model_var = adr,
                indep_var = apr                
                );


    %let errflg=0;



    * Detect Outliers *;
    proc sort data = &indsn;
        by prop_code dow ;
    run;


    proc summary data = &indsn noprint nway;
        class prop_code dow;
        output out = stat1(drop=_type_) 
           mean(&indep_var &model_var) = m_apr m_adr
           std (&indep_var &model_var) = std_apr std_adr
        ;
    run;


    data apr_rt3;
        merge &indsn 
              stat1; 
        by prop_code dow;

        if &indep_var < m_apr - (&sd_multiplier * std_apr) then delete; 
        else if &indep_var > m_apr + (&sd_multiplier * std_apr) then delete; 

        if &model_var < m_adr - (&sd_multiplier * std_adr) then delete; 
        else if &model_var > m_adr + (&sd_multiplier * std_adr) then delete; 

        array a1(6) dow2-dow7;
        do i=1 to 6;
            a1(i)=(dow=i+1);
        end;
        array a2(11) mon2-mon12;
        do i=1 to 11;
            a2(i)=(mon=i+1);
        end;
    run;


    data filtered_props ;
        merge &prop_code_ds (in = pc_)
              apr_rt3 (in = apr_) ;
        by prop_code;
        if pc_ and not apr_ then output;
        keep prop_code;
    run;

     
    %dataobs (filtered_props) ;
    %let num_filtered_props = &dataobs ;

        

    proc reg data = apr_rt3 outest=est3 rsquare TABLEOUT noprint;
        by prop_code;
        model &model_var = dow2-dow7 mon2-mon12 &indep_var;
    run;


    proc sort data = est3 out = est3_1;
        by prop_code;
        where _type_='PARMS';
    run;



    ** Check for any prop_codes missing from the estimation data set. **;

    data missing_props ;
        merge est3_1 (in = est_)
              &prop_code_ds (in=pc_)
              filtered_props (in=filt_);
        by prop_code;
        if pc_ and not est_ and not filt_ then output ;
        keep prop_code;
    run;

    %dataobs (missing_props) ;
    %if &dataobs > 0 %then %do ;

        %let num_missing_props = &dataobs ;
        
        data apr_rt4 ;
            merge missing_props (in = miss_)
                  apr_rt3 (in=apr_) ;
            if miss_ and apr_ ;
        run;


        proc reg data = apr_rt4 outest=est4 rsquare TABLEOUT noprint;
            by prop_code;
            model &model_var = dow2-dow7 &indep_var;
        run;

        %dataobs(est4) ;
        %if &dataobs > 0 %then %do;

            proc sort data = est4 out=est4_1 ;
                by prop_code ;
                where _type_ = 'PARMS' ;
            run;

            data still_missing ;
                merge missing_props (in=miss_)
                      est4_1 (in=est_) ;
                by prop_code ;
                if miss_ and not est_ then output ;
                keep prop_code;
            run;


            %dataobs (still_missing);
            %let num_missing_props = &dataobs;


            data est3_1 ;
                set est3_1 
                est4 (where=(_type_ = 'PARMS')) ;
            run;

        %end;
        %else %do;

           data still_missing ;
               set missing_props;
           run;

        %end;
        

    %end;
    %else %let num_missing_props = 0;


    data &outestdsn;
        length modelvar indepvar $40;

        set est3_1
            %if &num_missing_props > 0 %then
            still_missing_props  (in = miss_);
            %if &num_filtered_props > 0 %then 
            filtered_props (in = filt_);
        ;

        asofdate = "&lastdt9"d;
        format asofdate date9.;

        reg_failed_yn = 'N';
        %if &num_missing_props > 0 %then 
        if miss_ then reg_failed_yn = 'Y' %str(;) ;

        filtered_yn = 'N';
        %if &num_filtered_props > 0 %then 
        if filt_ then filtered_yn = 'Y' %str(;) ;

        rename 
            dow2=dow2_coef
            dow3=dow3_coef
            dow4=dow4_coef
            dow5=dow5_coef
            dow6=dow6_coef
            dow7=dow7_coef
            mon2=mon2_coef
            mon3=mon3_coef
            mon4=mon4_coef
            mon5=mon5_coef
            mon6=mon6_coef
            mon7=mon7_coef
            mon8=mon8_coef
            mon9=mon9_coef
            mon10=mon10_coef
            mon11=mon11_coef
            mon12=mon12_coef
            &indep_var=apr_coef;

        modelvar = "&model_var" ;
        indepvar = "&indep_var" ;

        keep prop_code intercept dow2-dow7 mon2-mon12 &indep_var 
             asofdate indepvar modelvar reg_failed_yn filtered_yn _rsq_;

    run;

    %submacrend:

%mend;
