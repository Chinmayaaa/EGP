/**
   \file
   \ingroup    SASUNIT_EXAMPLES_TEST

   \brief      Run all test scenarios for SASUnit example project

               (see also <A href="https://sourceforge.net/p/sasunit/wiki/User's%20Guide/" target="_blank">SASUnit User's Guide</A>)\n
               Initialize SASUnit (open test repository or create when needed) with initSASUnit.sas
               - set project name
               - set root path for convenience (all other paths can be relative to root path)
               - set paths for SASUnit macro library, test repository, units under test, test data, reference data

               Run all test scenarios in folder example\saspgm (suffix _test.sas) with runSASUnit.sas where
               the test scenario or the unit under test has been changed since previous run.            

               Create or recreate necessary HTML pages in the test report with reportSASUnit.sas.
               
   \version    \$Revision: 346 $
   \author     \$Author: klandwich $
   \date       \$Date: 2014-11-16 06:51:26 +0100 (So, 16 Nov 2014) $
   \sa         For further information please refer to <A href="https://sourceforge.net/p/sasunit/wiki/User's%20Guide/" target="_blank">SASUnit User's Guide</A>
   \sa         \$HeadURL: https://svn.code.sf.net/p/sasunit/code/tags/v1.5/example/saspgm/run_all.sas $
   \copyright  Copyright 2010, 2012 HMS Analytical Software GmbH.
               This file is part of SASUnit, the Unit testing framework for SAS(R) programs.
               For terms of usage under the GPL license see included file readme.txt
               or https://sourceforge.net/p/sasunit/wiki/readme.v1.2/.
*/ /** \cond */  

OPTIONS 
   MPRINT MAUTOSOURCE NOMLOGIC NOSYMBOLGEN
   SASAUTOS=(SASAUTOS "%sysget(SASUNIT_ROOT)/saspgm/sasunit") /* SASUnit macro library */
;

/* open test repository or create when needed */
%initSASUnit(
   i_root            = .                                                /* root path, all other paths can then be relative paths */
  ,io_target         = doc/sasunit/%lowcase(%sysget(SASUNIT_LANGUAGE))  /* Output of SASUnit: test repository, logs, results, reports */
  ,i_overwrite       = %sysget(SASUNIT_OVERWRITE)                       /* set to 1 to force all test scenarios to be run, else only changed 
                                                                           scenarios or scenarios with changed unit under test will be run*/
  ,i_project         = SASUnit Examples                                 /* Name of project, for report */
  ,i_sasunit         = %sysget(SASUNIT_ROOT)/saspgm/sasunit             /* SASUnit macro library */
  ,i_sasautos        = saspgm                                           /* Search for units under test here */
  ,i_testdata        = dat                                              /* test data, libref testdata */
  ,i_refdata         = dat                                              /* reference data, libref refdata */
  ,i_sascfg          = bin/sasunit.%sysget(SASUNIT_SAS_VERSION).%lowcase(%sysget(SASUNIT_HOST_OS)).%lowcase(%sysget(SASUNIT_LANGUAGE)).cfg
  ,i_testcoverage    = %sysget(SASUNIT_COVERAGEASSESSMENT)              /* set to 1 to assess test coverage assessment */
  ,i_verbose         = 1
)

/* Run specified test scenarios. There can be more than one call to runSASUnit */
%runSASUnit(i_source = saspgm/%str(*)_test.sas);

/* Create or recreate HTML pages for report where needed */
%reportSASUnit(
   i_language=%upcase(%sysget(SASUNIT_LANGUAGE))
  ,o_html=1
  ,o_junit=1
);

/** \endcond */
