data test_pop_general;
	set cohort.pop_general;
	ben_nai_ann_num = input(ben_nai_ann, 8.);
	run;

/*No NR fictif, at least one consummation dans SNDS*/
data cohort.pop_general_app; /*limiter la population pour faire l'appariement*/
	set test_pop_general;
	where TOP_FICTIF = 0 and TOP_CONSO_ALL = 1 and ben_nai_ann_num <2006
and TOP_JUMEAUX_MEME_SEXE = 0;
	run;

/*recoder les regions de residence*/
DATA cohort.pop_general_app;
SET cohort.pop_general_app;
if region = "32- Hauts de France" then region_code = 1;
if region = "84-Auvergne-Rhône Alpes" then region_code = 2;
if region = "93-Provence Alpes Cote d'Azur" then region_code = 3;
if region = "44-Grand Est" then region_code = 4;
if region = "76-Occitanie" then region_code = 5;
if region = "28- Normandie" then region_code = 6;
if region = "75-Nouvelle Aquitaine" then region_code = 7;
if region = "24-Centre-Val de Loire" then region_code = 8;
if region = "94-Corse" then region_code = 9;
if region = "27- Bourgogne Franche Comté" then region_code = 10;
if region = "53-Bretagne" then region_code = 11;
if region = "52-Pays de Loire" then region_code = 12;
if region = "11-Ile de France" then region_code = 13;
if region = "DOM/COM" then region_code = 14;
if region = "Inconnu" then region_code = 15;
run;

/*Recuperer identifiant, date index de chaque groupe a risque dans une table*/
proc sql;
create table set1 as select distinct
ben_idt_ano,
date_index as date_index_atcd_ei,
1 as top_ATCD_EI
from clean.groupe_atcd_ei_jul;
quit;

proc sql;
create table set2 as select distinct
ben_idt_ano,
date_index as date_index_PV,
1 as top_PV
from clean.groupe_pv_jul;
quit;

proc sql;
create table set3 as select distinct
ben_idt_ano,
date_index as date_index_MCM,
1 as top_MCM
from clean.groupe_mcm_jul;
quit;

proc sql;
create table set4 as select distinct
ben_idt_ano,
date_index as date_index_CIED,
1 as top_CIED
from clean.groupe_cied_jul;
quit;


proc sort data=set1; by ben_idt_ano; run;
proc sort data=set2; by ben_idt_ano; run;
proc sort data=set3; by ben_idt_ano; run;
proc sort data=set4; by ben_idt_ano; run;

data merge1;
	merge cohort.pop_general_app (in=a) set1 (in=b );
	by ben_idt_ano;
	if a;
	run;

proc sort data=merge1; by ben_idt_ano; run;

data merge2;
	merge merge1 (in=a) set2 (in=b );
	by ben_idt_ano;
	if a;
	run;

proc sort data=merge2; by ben_idt_ano; run;

data merge3;
	merge merge2 (in=a) set3 (in=b );
	by ben_idt_ano;
	if a;
	run;

proc sort data=merge3; by ben_idt_ano; run;

data merge4;
	merge merge3 (in=a) set4 (in=b );
	by ben_idt_ano;
	if a;
	run;

proc sort data=merge4; by ben_idt_ano; run;

data set_merge4;
	set merge4;
	if top_ATCD_EI = . then top_ATCD_EI = 0;
	if top_PV = . then top_PV = 0;
	if top_MCM = . then top_MCM = 0;
	if top_CIED = . then top_CIED = 0;
	run;

/*creer la table en commun*/
data cohort.app_all_groups (keep= ben_idt_ano id ben_sex_cod ben_nai_ann_num region_code top_conso_2008 top_conso_2009 top_conso_2010
top_conso_2011 top_conso_2012 top_conso_2013 top_conso_2014 top_conso_2015 top_conso_2016 top_conso_2017 top_conso_2018 top_conso_2019
top_conso_2020 top_conso_2021 top_conso_2022 top_conso_2023 top_ATCD_EI top_PV top_MCM top_CIED date_index_atcd_ei date_index_pv date_index_mcm date_index_cied
ben_dcd_dte date_deces_pmsi max_exe_soi_dtf_all);
	set set_merge4;
	run;

data cohort.app_all_groups;
	set cohort.app_all_groups;
	id = _N_; /*il faut utiliser id pour importer la table dans R*/
	run;


/*identifier la date deces*/
data set1_deces; 
	set cohort.app_all_groups;
	delai_pmsi_conso_max = max_exe_soi_dtf_all - date_deces_pmsi; /*max date conso ne doit pas depasser date deces pmsi 21 jours*/
	run;


data set2_deces; /*define date deces, if there is ben_dcd_dte we keep the value of this variable otherwise we use ben_dcd_dte*/
	set set1_deces;
	format date_deces_dcir_pmsi date9.;
	if not missing(ben_dcd_dte) or not missing(date_deces_pmsi) then deces_dcir_pmsi = 1;
	if missing(ben_dcd_dte) and missing(date_deces_pmsi) then deces_dcir_pmsi = 0;
	if not missing(ben_dcd_dte) then date_deces_dcir_pmsi = ben_dcd_dte;
	if missing(ben_dcd_dte) and not missing(date_deces_pmsi) and delai_pmsi_conso_max <= 21 then date_deces_dcir_pmsi = date_deces_pmsi;
	run;

data cohort.app_all_groups; set set2_deces; 
missing_date_deces = 0;
if deces_dcir_pmsi = 1 and missing(date_deces_dcir_pmsi) then missing_date_deces = 1; 
run;

data cohort.app_all_groups;
	set cohort.app_all_groups;
	where missing_date_deces = 0;
	run;

proc contents data=cohort.app_all_groups; run;

data chunk_data(keep=id date_index_atcd_ei date_index_mcm date_index_pv date_index_cied date_deces_dcir_pmsi 
BEN_SEX_COD ben_nai_ann_num region_code);
	set cohort.app_all_groups;
	run;


/*MACRO FOR CREATING DATASET CHUNK TO IMPORT TO R*/
%macro data_chunk_cut (data_source = , group = );

proc sql noprint;
    select count(*) into :total_obs from &data_source;
quit;

%let chunk_size = 1000000; /* Adjust the chunk size as needed */
%let num_chunks = %sysevalf((&total_obs + &chunk_size - 1) / &chunk_size, ceil);

%macro split_dataset;
    %do i = 1 %to &num_chunks;
        data chunk.&group._chunk_&i;
            set &data_source;
            if _n_ > (&i - 1) * &chunk_size and _n_ <= &i * &chunk_size;
        run;
    %end;
%mend;

%split_dataset;
%mend;

%data_chunk_cut(data_source = chunk_data, group=all);

data clean.cohorte_risque_1007 (keep=ben_idt_ano id top_atcd_ei top_pv top_mcm top_cied
date_index_atcd_ei date_index_pv date_index_mcm date_index_cied);
	set cohort.app_all_groups;
	where top_ATCD_EI = 1 or top_MCM = 1 or top_PV = 1 or top_CIED=1;
	run;

data set_atcd_s1 (keep= ben_idt_ano id date_index_atcd_ei date_index_pv date_index_mcm date_index_cied exclu_s3);
	set clean.cohorte_risque_1007;
	if not missing(date_index_PV) and date_index_PV <= date_index_atcd_ei then exclu_s3 = 1;
	if not missing(date_index_MCM) then exclu_s3 = 1;
	if not missing(date_index_CIED) and date_index_CIED <= date_index_atcd_ei then exclu_s3 = 1;
	where top_ATCD_EI = 1;
	run;

data set_pv_s1 (keep= ben_idt_ano id date_index_atcd_ei date_index_pv date_index_mcm date_index_cied exclu_s3);
	set clean.cohorte_risque_1007;
	if not missing(date_index_atcd_ei) and date_index_atcd_ei <= date_index_pv then exclu_s3 = 1;
	if not missing(date_index_MCM) then exclu_s3 = 1;
	if not missing(date_index_CIED) and date_index_CIED <= date_index_pv then exclu_s3 = 1;
	where top_PV = 1;
	run;

data set_mcm_s1 (keep= ben_idt_ano id date_index_atcd_ei date_index_pv date_index_mcm date_index_cied exclu_s3);
	set clean.cohorte_risque_1007;
	if not missing(date_index_atcd_ei) and date_index_atcd_ei <= date_index_mcm then exclu_s3 = 1;
	if not missing(date_index_pv) and date_index_pv <= date_index_mcm then exclu_s3 = 1;
	if not missing(date_index_CIED) and date_index_CIED <= date_index_mcm then exclu_s3 = 1;
	where top_mcm = 1;
	run;

data set_cied_s1 (keep= ben_idt_ano id date_index_atcd_ei date_index_pv date_index_mcm date_index_cied exclu_s3);
	set clean.cohorte_risque_1007;
	if not missing(date_index_atcd_ei) and date_index_atcd_ei <= date_index_cied then exclu_s3 = 1;
	if not missing(date_index_pv) and date_index_pv <= date_index_cied then exclu_s3 = 1;
	if not missing(date_index_mcm) then exclu_s3 = 1;
	where top_cied = 1;
	run;

data set_atcd_s3;
	set set_atcd_s1;
	top_atcd_s3 =1 ;
	where exclu_s3 = .; run;

data set_pv_s3;
	set set_pv_s1;
	top_pv_s3 = 1;
	where exclu_s3 = .; run;

data set_mcm_s3;
	set set_mcm_s1;
	top_mcm_s3 = 1;
	where exclu_s3 = .; run;


data set_cied_s3;
	set set_cied_s1;
	top_cied_s3 = 1;
	where exclu_s3 = .; run;

proc sort data=clean.cohorte_risque_1007; by ben_idt_ano; run;
proc sort data=set_atcd_s3; by ben_idt_ano; run;
proc sort data=set_pv_s3; by ben_idt_ano; run;
proc sort data=set_mcm_s3; by ben_idt_ano; run;
proc sort data=set_cied_s3; by ben_idt_ano; run;

data base1;
	merge clean.cohorte_risque_1007 (in=a) set_atcd_s3 (in=a keep=ben_idt_ano top_atcd_s3);
	by ben_idt_ano;
	run;

data base2;
	merge base1 (in=a) set_pv_s3 (in=a keep=ben_idt_ano top_pv_s3);
	by ben_idt_ano;
	run;

data base3;
	merge base2 (in=a) set_mcm_s3 (in=a keep=ben_idt_ano top_mcm_s3);
	by ben_idt_ano;
	run;

data base4;
	merge base3 (in=a) set_cied_s3 (in=a keep=ben_idt_ano top_cied_s3);
	by ben_idt_ano;
	run;

data clean.cohorte_risque_1007; /*j'ai fait petit a petit pour assurer tout est OK, je n'aurais pas besoin de refaire*/
	set base4;
	run;

/*importer la table pour les analyses sous-groupes*/
data chunk_data_ccam (keep=id ben_sex_cod region_code ben_nai_ann_num date_index_ccam_cied date_index_ccam_pv date_deces_dcir_pmsi);
	set cohort.app_all_groups; 
	run;

%data_chunk_cut(data_source = chunk_data_ccam, group=all);

