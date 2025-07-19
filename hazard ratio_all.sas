/*cox cause-specific model*/
/*Principal analysis*/
%macro HR_auto_main(groupe=, strat=);
data set_prepare_cas (keep= id exposure delai_endpoint_date endpoint_final);
    set clean.&groupe._cas_&strat.;
	delai_endpoint_date = intck('day', dom_&groupe., date_ep_final);
	exposure = 1;
run;

data set_prepare_controls (keep= id exposure delai_endpoint_date endpoint_final);
    set clean.&groupe._ctl_&strat.;
	delai_endpoint_date = intck('day', dom_&groupe., date_ep_final);
	exposure=0;
run;

data set_model;
	set set_prepare_cas set_prepare_controls;
	run;

ods output HazardRatios = HR_table;

proc phreg data=set_model;
	title 'Cause-Specific Hazard atcd';
	class exposure(order=internal ref=first);
	model delai_endpoint_date*Endpoint_final(3,2)=exposure;
	hazardratio exposure / cl=wald;
run;

ods output close;

/* Print the table */
/* Show only HR for exposure = 1 vs 0 */
data HR_final (keep=description groupe strat HR LowerHR UpperHR Exp_HR);
	set HR_table;
	length description $32.;
	description = "1 vs 0";
	groupe = "&groupe.";
	strat = "&strat.";
	HR = round(1/HazardRatio,0.1);
	LowerHR = round(1/WaldUpper,0.1);
	UpperHR = round(1/WaldLower,0.1);
	Exp_HR = CATS(HR, "(", LowerHR, "-", UpperHR, ")");
	run;

data clean.HR_non_ajuste;
	set clean.HR_non_ajuste HR_final;
	run;

proc datasets lib=work nolist;
    delete HR_table set_model set_prepare_cas set_prepare_controls;
quit;
%mend;

/*table stock*/
proc sql;
create table clean.HR_non_ajuste
(groupe char(10),
strat char(10),
description char(32),
HR num(10),
LowerHR num(10),
UpperHR num(10),
Exp_HR char(32));
quit;

%HR_auto_main(groupe=atcd, strat=s1);
%HR_auto_main(groupe=pv, strat=s1);
%HR_auto_main(groupe=mcm, strat=s1);
%HR_auto_main(groupe=cied, strat=s1);

%HR_auto_main(groupe=atcd, strat=s3);
%HR_auto_main(groupe=pv, strat=s3);
%HR_auto_main(groupe=mcm, strat=s3);
%HR_auto_main(groupe=cied, strat=s3);


%macro HR_auto_subgroup(groupe=, sousgroupe=);
data set_prepare_cas (keep= id exposure delai_endpoint_date endpoint_final);
    set clean.set_cas_&groupe._sub;
	delai_endpoint_date = intck('day', dom_ccam_&groupe., date_ep_final);
	exposure = 1;
	where top_index_&sousgroupe. = 1;
run;

data set_prepare_controls (keep= id exposure delai_endpoint_date endpoint_final);
    set clean.set_ctl_&groupe._sub;
	delai_endpoint_date = intck('day', dom_ccam_&groupe., date_ep_final);
	exposure=0;
	where top_index_&sousgroupe. = 1 and top_&groupe._ccam = 1;
run;

data set_model;
	set set_prepare_cas set_prepare_controls;
	run;
ods output HazardRatios = HR_table;

proc phreg data=set_model;
	title 'Cause-Specific Hazard atcd';
	class exposure(order=internal ref=first);
	model delai_endpoint_date*Endpoint_final(3,2)=exposure;
	hazardratio exposure / cl=wald;
run;

ods output close;

/* Print the table */
/* Show only HR for exposure = 1 vs 0 */
data HR_final (keep=description groupe strat HR LowerHR UpperHR Exp_HR);
	set HR_table;
	length description $32.;
	description = "1 vs 0";
	groupe = "&sousgroupe.";
	HR = round(1/HazardRatio,0.1);
	LowerHR = round(1/WaldUpper,0.1);
	UpperHR = round(1/WaldLower,0.1);
	Exp_HR = CATS(HR, "(", LowerHR, "-", UpperHR, ")");
	run;

data clean.HR_non_ajuste_sub;
	set clean.HR_non_ajuste_sub HR_final;
	run;

proc datasets lib=work nolist;
    delete HR_table set_model set_prepare_cas set_prepare_controls;
quit;
%mend;

proc sql;
create table clean.HR_non_ajuste_sub
(groupe char(10),
description char(32),
HR num(10),
LowerHR num(10),
UpperHR num(10),
Exp_HR char(32));
quit;

%HR_auto_subgroup(groupe=pv, sousgroupe=tavi);
%HR_auto_subgroup(groupe=pv, sousgroupe=mitra);
%HR_auto_subgroup(groupe=pv, sousgroupe=cec);

%HR_auto_subgroup(groupe=cied, sousgroupe=pm);
%HR_auto_subgroup(groupe=cied, sousgroupe=def);
%HR_auto_subgroup(groupe=cied, sousgroupe=pm_crt);
%HR_auto_subgroup(groupe=cied, sousgroupe=def_crt);




