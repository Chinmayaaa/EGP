******************************************************************;
** Program: read_revenue_plan.sas                               **;
**                                                              **;
** Purpose: Read revenue plan text file to obtain actual values **;
**          to be input into forecast calculations.             **;
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

%macro read_revenue_plan (planfref,
                prop_code_ds,
                outdsn,
                firstdt9,
                lastdt9,
                kdate,
                mdleft,
                adr_default_val,
                apr_default_val                
                );


    Data rp1;
        infile &planfref missover dsd lrecl=1100 ; 

        format D_run mmddyy10. inv_dt mmddyy10.;

        retain D_run ;

        if _N_ = 1 then do;

            input

            @1 D_run yymmdd10. ;

            put ' DEBUG: D_run ' D_run; 

            delete;

            return;

        end;

        input
           @1     prop_code $5. 
           @6     inv_dt yymmdd10.
           @16    AU_GRP_RMS_DEF  8.
           @24    AU_GRP_RMS_TNT  8.
           @32    AU_GRP_RMS_TOBE  8.
           @40    AU_TRN_RMS 8.
           @84    AU_CNT_RMS  8.
           @110   AU_GRP_REV_DEF  18.3
           @128   AU_GRP_REV_TNT  18.3
           @146   AU_GRP_REV_TOBE  18.3
           @200   AU_TRN_REV 18.3
           @218   AU_CNT_REV  18.3
        ;
        dl = inv_dt - D_run;

        if  dl >= 0 
        and inv_dt >= "&firstdt9"d 
       ;

        if prop_code in (
        %do k=1 %to &num_pc_lists ;
            &&pclist_&k
            %if &k < &num_pc_lists %then ,;
        %end;
        );

        if AU_TRN_RMS not in (.,0) then tran_adr=AU_TRN_REV/AU_TRN_RMS;
        else tran_adr=.;

        total_rev=sum(AU_GRP_REV_DEF,AU_GRP_REV_TNT,AU_GRP_REV_TOBE,AU_TRN_REV);
        total_rms=sum(AU_GRP_RMS_DEF,AU_GRP_RMS_TNT,AU_GRP_RMS_TOBE,AU_TRN_RMS);

        if total_rms not in (.,0) then apr=total_rev/total_rms;
        else apr = .;

        year = year(inv_dt);
        month = month(inv_dt) ;
 
        keep prop_code dl inv_dt tran_adr apr year month;

        rename inv_dt=inv_date;

    run;


    proc sort data = rp1 ;
        by prop_code year month inv_date ;
        where inv_date >= "&lastdt9"d ;
    run;



    ** Look for missing revenue plan dates for each prop_code **;

    data missing_plan_dates ;
        set rp1;
        by prop_code inv_date;

        retain last_date 0;
        
        if first.prop_code then do ;
            last_date = inv_date ;

            if inv_date ne "&lastdt9"d and "&lastdt9"d < inv_date then 
            do i = "&lastdt9"d to (inv_date -1) ;
                missing_date = i ;
                year = year(i);
                month = month(i) ;
                output ;
            end;
        end;
        else if last.prop_code and inv_date ne ("&lastdt9"d + &mdleft) then 
            do i = (inv_date) to ("&lastdt9"d + &mdleft);
            missing_date = i ;
            year = year(i);
            month = month(i) ;
            output ;
        end;
        else if inv_date ne (last_date + 1) then 
           do i = (last_date +1) to (inv_date - 1);
            missing_date = i ;
            year = year(i);
            month = month(i) ;
            output ;
        end;
        else if apr + tran_adr = . then do;
            missing_date = inv_date;
            output ;
        end;
 
        last_date = inv_date ;

        format missing_date date9.;

        keep prop_code missing_date year month ;
    run;


        
    %dataobs (missing_plan_dates);
    %if &dataobs > 0 %then %do;

         ** Create distinct list of prop_codes and months for which missing data **;
         ** occurred.                                                            **;
    
         proc sql ;

             create table distinct_miss_pc_months
             as select distinct prop_code,
                                year,
                                month
             from missing_plan_dates 
             order by prop_code, year, month ;

         quit;



         ** Subset the revenue plan data for months in which missing **;
         ** date values were found.                                  **;

         data month_summary_input 
              still_missing_months ;

             merge rp1 (in=rp_)
                   distinct_miss_pc_months (in=miss_) end=eof;
             by prop_code year month ;

             if rp_ and not miss_ then delete;
             else if rp_ and miss_ then output month_summary_input;
             else if miss_ then output still_missing_months;
             
         run;

   

         ** Obtain monthly property averages **;
         proc summary data = month_summary_input nway noprint;
             class prop_code year month;
             var tran_adr apr ;
             output out = pc_month_summary mean = pc_adr_mean pc_apr_mean ;
         quit;



        ** Join the property month averages with the missing dates **;

        data missing_date_means ;
             merge missing_plan_dates (in=miss_)
                   pc_month_summary (in=sum_);
             by prop_code year month ;
             if miss_ and sum_;
             rename missing_date = inv_date ;
             drop _type_ _freq_ ;
        run;


        proc sort data=missing_date_means ;
            by prop_code year month inv_date;
        run;
 

        %dataobs(still_missing_months) ;
        %let still_missing_count = &dataobs;

        %if &dataobs > 0 %then %do;

            ** Obtain monthly overall averages, if needed **;

            proc sql ;

                create table distinct_missing_years
                as select distinct prop_code, year
                from still_missing_months ;

            quit;


        
            ** Obtain prop_code, year averages **;
 
            proc summary data = rp1 noprint ;
                class prop_code year ;
                var tran_adr apr ;
                output out = year_summary (where = (_type_ in(2,3))) 
                       mean = adr_mean apr_mean ;
            quit;


            ** Put the property, year and property-only averages in the same records **;

            data pc_year_summary ;
                merge year_summary (where = (_type_ = 2)
                                   rename = (adr_mean = pc_overall_adr_mean 
                                             apr_mean = pc_overall_apr_mean) )
                      year_summary (where = (_type_ = 3)) ;
                by prop_code ;
                drop _type_ _freq_ ;
            run;
     
        %end;



        data %if &still_missing_count > 0 %then outdsn1;
             %else &outdsn ;
            ;
            merge missing_date_means 
                  rp1;
            by prop_code year month inv_date;

            default_apr_rate = 'N';
            if apr = . and pc_apr_mean ne . then do;
                apr = pc_apr_mean;
                default_apr_rate = 'M';
            end;

            default_adr_rate = 'N';       
            if tran_adr = . and pc_adr_mean ne . then do;
                tran_adr = pc_adr_mean;
                default_adr_rate = 'M';
            end;
         
            drop pc_apr_mean pc_adr_mean ;

        run;



        %if &still_missing_count > 0 %then %do;

            proc sql;

                create table outdsn2
                as select rp.*,
                          ms.adr_mean,
                          ms.apr_mean,
                          ms.pc_overall_adr_mean,
                          ms.pc_overall_apr_mean
                from outdsn1 rp
                     left join pc_year_summary ms
                on  rp.prop_code = ms.prop_code
                and rp.year = ms.year 
                order by rp.prop_code,
                         rp.inv_date
                ;
               
            quit;
            


            data &outdsn ;
                set outdsn2 ;

                by prop_code inv_date ;

                if apr = . and apr_mean ne . then do;
                    apr = apr_mean;
                    default_apr_rate = 'A';
                end;
                else if apr = . and pc_apr_mean ne . then do;
                    apr = pc_overall_apr_mean;
                    default_apr_rate = 'P';
                end;
                else if apr = . then do;
                    apr = &apr_default_val;
                    default_apr_rate = 'Y';
                end;

                if tran_adr = . and adr_mean ne . then do;
                    tran_adr = adr_mean;
                    default_adr_rate = 'A';
                end;
                else if tran_adr = . and pc_adr_mean ne . then do;
                    tran_adr = pc_overall_adr_mean;
                    default_adr_rate = 'P';
                end;
                else if tran_adr = . then do ;
                    tran_adr = &adr_default_val;
                    default_adr_rate = 'Y';
                end;
    
                drop adr_mean pc_overall_adr_mean apr_mean pc_overall_apr_mean ;
 
            run;

        %end;

    %end;
    %else %do;

         data &outdsn ;
             set rp1 ;
         run;

    %end;

    %submacrend:

%mend;
