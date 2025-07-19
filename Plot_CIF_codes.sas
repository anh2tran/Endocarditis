/*Cumulative incidence*/
proc format;
    value $grouplabel
        'atcd' = 'Prior IE'
        'pv' = 'Prosthetic valve'
		'cied' = 'CIED'
        'mcm' = 'CHD'
		'ta' = 'TAVI'
        'mitra' = 'Mitraclip'
		'cec' = 'Thoracotomy with ECC'
		'pm' = 'PM'
		'def' = 'DEF'
		'pm_crt' = 'PM+CRT'
		'def_rt' = 'DEF+CRT';
run;


%macro get_cif(groupe= ,strat=);
data set_prepare_cas (keep= id exposure delai_endpoint_date endpoint_final);
    set clean.&groupe._cas_&strat.;
	delai_endpoint_date = intck('day', dom_&groupe., date_ep_final);
	exposure = 1;
run;

proc lifetest data=set_prepare_cas plots=none conftype=loglog
                  error=aalen outcif=cif;
        time delai_endpoint_date*Endpoint_final(3) / failcode=1;
    run;

data cif_&groupe.;
        set cif;
		length groupe $32.;
        groupe = "&groupe.";
    run;

data set_prepare_controls (keep= id exposure delai_endpoint_date endpoint_final);
    set clean.&groupe._ctl_&strat.;
	delai_endpoint_date = intck('day', dom_&groupe., date_ep_final);
	exposure=0;
run;


proc lifetest data=set_prepare_controls plots=none conftype=loglog
                  error=aalen outcif=cif;
        time delai_endpoint_date*Endpoint_final(3) / failcode=1;
    run;

data cif_&groupe._control;
        set cif;
		length groupe $32.;
        groupe = "&groupe.";
    run;
%mend;

%get_cif(groupe=atcd, strat=s1);
%get_cif(groupe=pv, strat=s1);
%get_cif(groupe=mcm, strat=s1);
%get_cif(groupe=cied, strat=s1);


data cif_all_exposed;
    set cif_atcd cif_pv cif_mcm cif_cied;
    CIF_pct = CIF * 100;
run;

data cif_all_control;
    set cif_atcd_control cif_pv_control cif_mcm_control cif_cied_control;
    CIF_pct = CIF * 100;
run;

data cif_all_exposed;
	set cif_all_exposed;
	n_atrisk = .;
	tatrisk = .;
	if delai_endpoint_date in (0, 1826, 3652) then n_atrisk = Atrisk;
	if delai_endpoint_date in (0, 1826, 3652) then tatrisk = delai_endpoint_date;
	/*if delai_endpoint_date = 1825 and groupe = "at_p" then n_atrisk = Atrisk;
	if delai_endpoint_date = 1825 and groupe = "at_p" then tatrisk = 1825;*/
	if delai_endpoint_date = 3651 and groupe = "mcm" then n_atrisk = Atrisk;
	if delai_endpoint_date = 3651 and groupe = "mcm" then tatrisk = 3652;/*as there is not 3652 for atcd s3*/
	run;

data cif_all_control;
	set cif_all_control;
	n_atrisk = .;
	tatrisk = .;
	if delai_endpoint_date in (0, 1826, 3652) then n_atrisk = Atrisk;
	if delai_endpoint_date in (0, 1826, 3652) then tatrisk = delai_endpoint_date;
	if delai_endpoint_date = 3651 and groupe = "mcm" then n_atrisk = Atrisk;
	if delai_endpoint_date = 3651 and groupe = "mcm" then tatrisk = 3652;
	/*if delai_endpoint_date = 3651 and groupe = "atcd" then n_atrisk = Atrisk;
	if delai_endpoint_date = 3651 and groupe = "atcd" then tatrisk = 3652;*/ /*as there is not 3652 for atcd s3*/
	run;


proc sgplot data=cif_all_exposed;
styleattrs datacontrastcolors=(red blue green orange); /* Custom line colors */
    step x=delai_endpoint_date y=CIF_pct / group=groupe lineattrs=(thickness=2);
	xaxistable n_atrisk  / class=groupe x=tatrisk ;
    xaxis label="Time since index date (years)" 
          values=(0 365 730 1095 1461 1826 2191 2556 2922 3287 3652 4017 4382)
          valuesdisplay=("0" "1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "11" "12");
    yaxis label="Cumulative Incidence (%)" min=0 max=20 values=(0 to 10 by 1);
	keylegend / title="Exposed groups" location=outside position=topright;
    format groupe $grouplabel.;
run;

proc sgplot data=cif_all_control;
styleattrs datacontrastcolors=(red blue green orange); /* Custom line colors */
    step x=delai_endpoint_date y=CIF_pct / group=groupe lineattrs=(thickness=2);
	xaxistable n_atrisk  / class=groupe x=tatrisk ;
    xaxis label="Time since index date (years)" 
          values=(0 365 730 1095 1461 1826 2191 2556 2922 3287 3652 4017 4382)
          valuesdisplay=("0" "1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "11" "12");
    yaxis label="Cumulative Incidence (%)" min=0 max=20 values=(0 to 10 by 1);
	keylegend / title="Control groups" location=outside position=topright;
    format groupe $grouplabel.;
run;




