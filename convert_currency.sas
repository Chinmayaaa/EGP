******************************************************************;
** Program: convert_currency.sas                                **;
**                                                              **;
** Purpose: Convert tran_adr (indep var) values from the        **;
**          currency code rates were sold to the currency       **;
**          that the property should be using, as defined in    **;
**          OYT_PROPERTY - in the case where they are different.**;
**                                                              **;
** Adapted from: do_currency_conversion macro of RPO program    **;
**               pull_rubicon_data_v7_MACROS.sas.               **;
**                                                              **;
** Pre-reqs: Libref pointed to by db2lib parameter should       **;
**           alread be assigned.                                **;
**           MI Currency should be stored in the input data     **;
**           set in the 'mi_currency' column.                   **;
**           The [trans_adr] (indep_var) currency should be     **;
**           stored in the 'currency' column.                   **;
**                                                              **;
** By:       Julia Morrison, Andrew Hamilton, July 2014.        **;
**                                                              **;
******************************************************************;

%macro convert_currency (indata, 
                         db2lib,
                         indep_var_col,
                         input_date_col,
                         other_columns,
                         outdata);

    data good_currencies 
         bad_currencies 
         no_currencies;
        set &indata;

        if ( upcase(compress(MI_currency)) ne upcase(compress(currency)) and currency ne '') 
         then output bad_currencies;
        else if(compress(currency) ne '') then 
         output good_currencies;
        else output no_currencies;
    run;


    %dataobs (bad_currencies) ;
    %let bad_currency_obs = &dataobs;
    %dataobs (no_currencies) ;
    %let no_currency_obs = &dataobs;

    %if %eval(&bad_currency_obs + &no_currency_obs) = 0 %then %do;

        %if &indata ne &outdata %then %do;

             data &outdata ;
                 set &indata ;
             run;

        %end;

        %put No bad / missing currencies to convert ;

        %goto submacrend ;

    %end;



    ** Figure out last day for which exchange rates are available in the exchange rates table **;

    proc sql noprint;
        select max(ID_TIME_PER_YEAR) into:max_exch_rates_year from &db2lib..MMT_CC_EXCH_RT;

        select min(ID_TIME_PER_YEAR) into:min_exch_rates_year from &db2lib..MMT_CC_EXCH_RT;

        select max(ID_TIME_PER_NUM) into:max_exch_rates_day from &db2lib..MMT_CC_EXCH_RT
        where CD_CALENDAR_TYPE = 'C' 
        and   CD_TIME_PER_TYPE ='D' 
        and ID_TIME_PER_YEAR = &max_exch_rates_year ;

    quit;

    %put &max_exch_rates_year;
    %put &max_exch_rates_day;



    ** This step prepares the keys necessary to lookup exchange rates         **;
    ** The exchange rates go back 3 years                                     **;
    ** For exchange rates that are more historical than that, a rate for PD 1 **;
    ** three years ago is used.                                               **;
    ** This rule is currently hard coded - need to follow up with MMRS        **;
    ** on their strategy for archiving exchange rates                         **;

    data bad_currencies;

        set bad_currencies;

        shop_year = year(&input_date_col);
        shop_doy = &input_date_col - MDY( 1, 1, shop_year ) +1 ;

        * Rule: use daily match for where we have data. For any date after the *;
        * last date with exchange rate, use the exchange rates for that last   *;
        * day, otherwise we use PD1 value for whenever &min_exch_rates_year is *;    

        if (shop_year < &min_exch_rates_year) then do;
            calendar_type = 'A';
            time_per_type = 'P';
            time_per_num = 1;
            time_per_year = &min_exch_rates_year;
        end;
        else if ( (shop_year > &max_exch_rates_year) or 
                  (shop_year = &max_exch_rates_year and shop_doy > &max_exch_rates_day )) 
        then do;
            calendar_type = 'C';
            time_per_type = 'D';
            time_per_num = &max_exch_rates_day;
            time_per_year = &max_exch_rates_year;
        end;
        else do;
            calendar_type = 'C';
            time_per_type = 'D';
            time_per_num = shop_doy;
            time_per_year = shop_year;
        end;

    run;



    ** This is the main SQL that appends two exchange rates: one for MI currency to USD, **;
    ** and another for Rubicon currency to USD.                                          **;

    proc sql;

        create table bad_currencies_conv
        as select &other_columns, 
                  &input_date_col,
                  &indep_var_col,
                  CURRENCY,
                  MI_CURRENCY,
                  MI_EX_RATE_PER_USD,
                  CASE WHEN CURRENCY ='USD' THEN 1
                       ELSE C.CU_EX_RATE_PER_USD
                       END as EX_RATE_PER_USD

        from (select &other_columns,
                     &input_date_col,
                     &indep_var_col,
                     CURRENCY,
                     MI_CURRENCY,
                     CALENDAR_TYPE,
                     TIME_PER_TYPE,
                     TIME_PER_NUM,
                     TIME_PER_YEAR,
                     CASE WHEN MI_CURRENCY ='USD' THEN 1
                     ELSE  b.CU_EX_RATE_PER_USD
                     END as MI_EX_RATE_PER_USD 
              from bad_currencies a 
              left join &db2lib..MMT_CC_EXCH_RT b
              on  a.MI_CURRENCY   = b.ID_ISO_CURRENCY_CD
              and a.CALENDAR_TYPE = b.CD_CALENDAR_TYPE
              and a.TIME_PER_TYPE = b.CD_TIME_PER_TYPE
              and a.TIME_PER_NUM  = b.ID_TIME_PER_NUM
              and a.TIME_PER_YEAR = b.ID_TIME_PER_YEAR 
        ) as tmp
        left join &db2lib..MMT_CC_EXCH_RT c
        on  tmp.CURRENCY      = c.ID_ISO_CURRENCY_CD
        and tmp.CALENDAR_TYPE = c.CD_CALENDAR_TYPE
        and tmp.TIME_PER_TYPE = c.CD_TIME_PER_TYPE
        and tmp.TIME_PER_NUM  = c.ID_TIME_PER_NUM
        and tmp.TIME_PER_YEAR = c.ID_TIME_PER_YEAR
        ;

    quit;



    ** Last step, recompute rates based on the exchange rates that we looked up **;
    ** potentially later fit into sql above later.                              **;
    
    data bad_currencies2;
        set bad_currencies_conv;
        currency = MI_currency;
        &indep_var_col = &indep_var_col * MI_EX_RATE_PER_USD / EX_RATE_PER_USD;
        drop MI_EX_RATE_PER_USD EX_RATE_PER_USD;
    run;



    ** Append back to the final dataset. MI_currency field is dropped by everyone **;

    data &outdata;
        set good_currencies 
            bad_currencies2 ;
    run; 


    %submacrend:


%mend;


