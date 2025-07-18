/*Programmes pour creer la cohorte CIED juillet2025*/
data results.conso_pat_cied_ccam_corrected; /*Corrige les codes ccam SELON mis a jour du protocole avant le depart de Sophie*/
	set results.conso_pat_defi_ccam2;
	where ACTE_CCAM in ("DELF001",
"DELF005",
"DELF007",
"DELF010",
"DELF012",
"DELF015",
"DELF223",
"DELF901",
"DELF902",
"DELF903",
"DELF904",
"DELF905",
"DEKA001",
"DELF017",
"DELF018",
"DELF019",
"DEEF001",
"DEEF002",
"DEEA001",
"DEMP001",
"DEMP002",
"DERP001",
"DELA007",
"DELF013",
"DELF014",
"DELF016",
"DELF020",
"DELF900",
"DEKA002"); run;

/*Joindre les cohortes construites à partir cim et ccam*/
proc sql;
create table conso_defi_ccam as select distinct
a.ben_idt_ano,
a.date_execution as date_execution_ccam,
a.date_execution as date_entree_cohorte format=date9., /*date execution = date index corrigé 10/07*/
year(a.date_execution) as annee_execution,
b.ben_nai_ann,
b.TOP_FICTIF,
b.TOP_JUMEAUX_MEME_SEXE
from results.conso_pat_cied_ccam_corrected a, cohort.pop_general b
where a.ben_idt_ano = b.ben_idt_ano and a.date_execution <= "30JUN2023"d;
quit;

proc sql;
create table conso_defi_cim as select distinct
a.ben_idt_ano,
a.date_execution as date_execution_cim,
a.exe_soi_dtf as date_entree_cohorte format=date9.,/*Parce que pour les séjours hospitliers, on compte a partir de date de sortie*/
year(a.exe_soi_dtf) as annee_execution,
b.ben_nai_ann,
b.TOP_FICTIF,
b.TOP_JUMEAUX_MEME_SEXE
from results.conso_pat_defi_cim a, cohort.pop_general b
where a.ben_idt_ano = b.ben_idt_ano and a.exe_soi_dtf <= "30JUN2023"d; /*date sortie avant*/
quit;

data conso_ccam;
	set conso_defi_ccam;
	where TOP_FICTIF = 0 and TOP_JUMEAUX_MEME_SEXE = 0;
	run;

data conso_cim;
	set conso_defi_cim;
	where TOP_FICTIF = 0 and TOP_JUMEAUX_MEME_SEXE = 0;
	run;

proc sql;
	create table table_n_1 as select count(distinct ben_idt_ano) as N_pat, "cied_ccam" as step length=32
	from conso_ccam;
	quit;

proc sql;
	create table table_n_2 as select count(distinct ben_idt_ano) as N_pat, "cied_cim" as step length=32
	from conso_cim;
	quit;

/*Restreindre aux patients >= 18 ans*/
data conso_defi_ccam_adulte;
	set conso_ccam;
	age_exe = annee_execution - ben_nai_ann;
	run;

data conso_defi_cim_adulte;
	set conso_cim;
	age_exe = annee_execution - ben_nai_ann;
	run;


proc sql;
create table tab_top_defi_ccam as select distinct
ben_idt_ano,
min(date_entree_cohorte) as date_entree_cohorte_ccam format=date9.,
min(date_execution_ccam) as date_execution_min_ccam format=date9.,
1 as top_defi_ccam
from conso_defi_ccam_adulte
where age_exe >=18 and ben_nai_ann ne "0000"
group by ben_idt_ano;
quit;


proc sql;
create table tab_top_defi_cim as select distinct
ben_idt_ano,
min(date_entree_cohorte) as date_entree_cohorte_cim format=date9., /*calculé à partir de date sortie*/
min(date_execution_cim) as date_execution_min_cim format=date9.,
1 as top_defi_cim
from conso_defi_cim_adulte
where age_exe >= 18 and ben_nai_ann ne "0000"
group by ben_idt_ano;
quit;

/*Compte N >=18 ans*/
proc sql;
create table table_n_3 as select count(distinct ben_idt_ano) as N_pat, "ccam_18ans" as step length=32
from tab_top_defi_ccam;
quit;

proc sql;
create table table_n_4 as select count(distinct ben_idt_ano) as N_pat, "cim_18ans" as step length=32
from tab_top_defi_cim;
quit;

/*Merger 2 cohortes ccam et cim*/
data tab_defi_1;
	merge cohort.pop_general (in=a) tab_top_defi_ccam(in=b);
	if a;
	by ben_idt_ano;
	run;

data tab_defi2;
	merge tab_defi_1 (in=a) tab_top_defi_cim(in=b);
	if a;
	by ben_idt_ano;
	run;

data tab_defi;
	set tab_defi2;
	where top_defi_ccam = 1 or top_defi_cim =1;
	run;

/*Compte N de cohorte mergé*/
proc sql;
create table table_n_5 as select count(distinct ben_idt_ano) as N_pat, "total" as step length=32
from tab_defi;
quit;

data tab_defi_temps;
	set tab_defi;
	format date_entree_cohorte date9.;
	if missing(top_defi_cim) then date_entree_cohorte = date_entree_cohorte_ccam;
	if missing(top_defi_ccam) then date_entree_cohorte = date_entree_cohorte_cim;
	if top_defi_ccam = 1 and top_defi_cim = 1 then 
	date_entree_cohorte = min(date_entree_cohorte_ccam, date_entree_cohorte_cim);
	run;

/*date execution final = date execution min for ccam, date sortie min for cim-10*/

data tab_defi_temps_1;
	set tab_defi_temps;
	no_ccam_exact =.;
	if missing(top_defi_ccam) then no_ccam_exact = 1;
	if top_defi_cim = 1 and top_defi_ccam = 1 and date_execution_min_cim < date_execution_min_ccam 
then no_ccam_exact = 1;
	run;

data tab;
set tab_defi_temps_1;
class_ccam= 0; /*only ccam*/
class_cim=0; /*only cim*/
class_ccam_before = 0; /*ccam before cim*/
class_cim_before=0; /*cim before ccam*/
if missing(top_defi_cim) and top_defi_ccam=1 then class_ccam = 1;;
if missing(top_defi_ccam) and top_defi_cim=1 then class_cim =1;
if top_defi_cim = 1 and top_defi_ccam = 1 and date_execution_min_cim < date_execution_min_ccam then class_cim_before = 1;; 
if top_defi_cim = 1 and top_defi_ccam = 1 and date_execution_min_cim >= date_execution_min_ccam then class_ccam_before = 1; 
run;

/*Compte N*/
proc sql;
create table work.compte_chaque_classe as select
sum(class_ccam) as N_only_ccam,
sum(class_cim) as N_only_cim,
sum(class_ccam_before) as N_ccam_before_cim,
sum(class_cim_before) as N_cim_before_ccam
from tab;
quit;


data tab_defi_temps_1; set tab_defi_temps_1; annee_execution = year(date_entree_cohorte); where ben_nai_ann ne "0000"; run;

%macro creat_merge_data_conso();
%do i=2006 %to 2023;
%let year=&i;
data tab_defi_temps_1;
	set tab_defi_temps_1;
	top_conso_&year = 0;
	if top_conso_dcir_&year = 1 or top_conso_pmsi_&year = 1 then top_conso_&year = 1;
run;
%end;
%mend creat_merge_data_conso;

%creat_merge_data_conso;


data conso_2ans_av;
    set tab_defi_temps_1;
    format conso_year_n_1 32. conso_year_n_2 32.;

    /* Loop over the years */
    array top_conso_array{15} top_conso_2022 top_conso_2021 top_conso_2020 
                                   top_conso_2019 top_conso_2018 top_conso_2017 top_conso_2016 
                                   top_conso_2015 top_conso_2014 top_conso_2013 top_conso_2012 
                                   top_conso_2011 top_conso_2010 top_conso_2009 top_conso_2008;

    if 2010 <= annee_execution <= 2023 then do;
        conso_year_n_1 = top_conso_array{2024 - annee_execution};
        conso_year_n_2 = top_conso_array{2025 - annee_execution};
    end;

run;

data conso_2ans_av; /*il y a ben_idt_ano et aussi ben_nir_psa*/
	set conso_2ans_av;
	top_conso_2ans_avant = 0;
	if conso_year_n_1 = 1 or conso_year_n_2 = 1 then top_conso_2ans_avant = 1;
	run;


/*Cette cohorte a été déjà appliquée tous les critères: adultes >= 18ans, avec consommation dans SNDS
	dans 2 ans, pas NIR fictif*/
proc sql;
create table cohort.cied_july as select distinct
ben_idt_ano,
date_entree_cohorte as date_index format=date9.,
date_execution_min_ccam,
date_execution_min_cim,
date_entree_cohorte_ccam,
date_entree_cohorte_cim,
top_defi_ccam,
top_defi_cim,
no_ccam_exact,
date_deces_pmsi,
ben_dcd_dte,
MAX_EXE_SOI_DTF_ALL,
1 as top_defi_pacemaker
from conso_2ans_av
where top_conso_2ans_avant = 1;
quit;

/*Compte N cohort défibrillateur pacemakers*/
proc sql;
create table table_n_6 as select count(distinct ben_idt_ano) as N_pat, "conso" as step length=32
from cohort.cied_july; quit;

data work.set1;
	set cohort.cied_july;
	run;

/*IDENTIFIER LA DATE DE DECES BASEE SUR DCIR ET PMSI*/
%let cohort_entree = work.set1; /*assurer qu'il y a 3 variables: ben_dcd_dte, date_deces_pmsi, et max_exe_soi_dtf_all*/

/*Delai entre date deces pmsi and max date consommation*/
data set1_deces; 
	set &cohort_entree.;
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

data set3_deces; set set2_deces; 
if deces_dcir_pmsi = 1 and missing(date_deces_dcir_pmsi) then missing_date_deces = 1; 
if deces_dcir_pmsi = 1 and not missing(date_deces_dcir_pmsi) and date_deces_dcir_pmsi <= date_index then deces_avant_index = 1; run;
run;

data clean.groupe_cied_jul;
	set set3_deces;
	where deces_avant_index = . and missing_date_deces = .; run; /*no deces avant index and no missing date deces*/

proc sql;
	create table table_n_7 as select count(distinct ben_idt_ano) as N_pat, "final" as step length=32
	from clean.groupe_cied_jul; quit;

data table_flowchart;
	set table_n_1 table_n_2 table_n_3 table_n_4 table_n_5 table_n_6 table_n_7; run;


/**Identifier sous-groupes-------------------------------------------------------------------------**/
data set1 (keep=ben_idt_ano date_index date_deces_dcir_pmsi);
	set clean.groupe_cied_jul;
	run;

proc sql;
create table ccam_cied as select distinct
a.*,
b.date_index
from cohort.conso_ccam_cied as a
inner join set1 as b
        on a.ben_idt_ano = b.ben_idt_ano and
		a.date_execution >= b.date_index;
quit;

proc sort data=ccam_cied; by ben_idt_ano date_execution; run;

data ccam_cied_2;
	set ccam_cied;
	by ben_idt_ano;
	if first.ben_idt_ano then order_ccam = 1;
	else order_ccam + 1;
	run;

data first_ccam_cied (rename=(date_execution = date_index_ccam));
	set ccam_cied_2;
	where order_ccam = 1;
	run;

data first_ccam_cied;
	set first_ccam_cied;
	top_index_pm = 0;
	top_index_def = 0;
	top_index_pm_crt = 0;
	top_index_def_crt = 0;
	if ACTE_CCAM in ("DEMP002",
"DELF005",
"DEKA001",
"DELF007",
"DELF010",
"DELF017",
"DEEF001",
"DELF223",
"DELF019",
"DELF902",
"DELF903"
) then top_index_pm = 1;
if ACTE_CCAM in ("DEMP001",
"DEKA002",
"DELF013",
"DELF016",
"DERP001",
"DELF018",
"DEEA001",
"DEEF002",
"DELA007",
"DELF900"
) then top_index_def = 1;
if ACTE_CCAM in ("DELF015",
"DELF001",
"DELF012",
"DELF901",
"DELF905",
"DELF904"
) then top_index_pm_crt = 1;
if ACTE_CCAM in ("DELF014",
"DELF020"
) then top_index_def_crt = 1;
run;

/*uniquement un type de CCAM*/
data ccam_cied;
	set ccam_cied;
	count_not_pm = 1;
	count_not_def = 1;
	count_not_pm_crt = 1;
	count_not_def_crt = 1;
	if ACTE_CCAM in ("DEMP002",
"DELF005",
"DEKA001",
"DELF007",
"DELF010",
"DELF017",
"DEEF001",
"DELF223",
"DELF019",
"DELF902",
"DELF903"
) then count_not_pm = 0; 
if ACTE_CCAM in ("DEMP001",
"DEKA002",
"DELF013",
"DELF016",
"DERP001",
"DELF018",
"DEEA001",
"DEEF002",
"DELA007",
"DELF900"
) then count_not_def = 0;
if ACTE_CCAM in ("DELF015",
"DELF001",
"DELF012",
"DELF901",
"DELF905",
"DELF904"
) then count_not_pm_crt = 0;
if ACTE_CCAM in ("DELF014",
"DELF020"
) then count_not_def_crt = 0; run;

proc sql;
create table top_subgroup_cied_b as select distinct
ben_idt_ano,
date_index,
sum(count_not_pm) as top_not_pm_b,
sum(count_not_def) as top_not_def_b,
sum(count_not_pm_crt) as top_not_pm_crt_b,
sum(count_not_def_crt) as top_not_def_crt_b
from ccam_cied
group by ben_idt_ano;
quit;

data top_subgroup_cied (drop=top_not_pm_b top_not_def_b top_not_pm_crt_b top_not_def_crt_b);
	set top_subgroup_cied_b;
	top_not_pm=top_not_pm_b;
	top_not_def = top_not_def_b;
	top_not_pm_crt = top_not_pm_crt_b;
	top_not_def_crt = top_not_def_crt_b;
	if top_not_pm_b > 1 then top_not_pm = 1;
	if top_not_def_b > 1 then top_not_def = 1;
	if top_not_pm_crt_b > 1 then top_not_pm_crt = 1;
	if top_not_def_crt_b > 1 then top_not_def_crt = 1;
	run;

data top_subgroup_cied (drop=top_not_pm top_not_def top_not_pm_crt top_not_def_crt);
	set top_subgroup_cied;
	top_pm_unique = 1;
	top_def_unique = 1;
	top_pm_crt_unique = 1;
	top_def_crt_unique = 1;
	if top_not_pm = 1 then top_pm_unique = 0;
	if top_not_def = 1 then top_def_unique = 0;
	if top_not_pm_crt = 1 then top_pm_crt_unique = 0;
	if top_not_def_crt = 1 then top_def_crt_unique = 0;
	run;

proc sort data=top_subgroup_cied; by ben_idt_ano; run;
proc sort data=first_ccam_cied; by ben_idt_ano;

data clean.subgroup_cied_july;
	merge top_subgroup_cied (in=a) first_ccam_cied (in=a keep=ben_idt_ano 
top_index_pm top_index_def top_index_pm_crt top_index_def_crt date_index_ccam);
	by ben_idt_ano;
	if a;
	run;

proc freq data=clean.subgroup_cied_july; 
table top_pm_unique top_def_unique top_pm_crt_unique top_def_crt_unique top_index_pm top_index_def top_index_pm_crt top_index_def_crt; run;

proc sort data=clean.groupe_cied_jul; by ben_idt_ano; run;
proc sort data=clean.subgroup_cied_july; by ben_idt_ano; run;

data clean.groupe_cied_jul;
	merge clean.groupe_cied_jul (in=a) clean.subgroup_cied_july (in=b keep=ben_idt_ano top_pm_unique top_def_unique top_pm_crt_unique top_def_crt_unique top_index_pm 
top_index_def top_index_pm_crt top_index_def_crt date_index_ccam);
by ben_idt_ano; 
if a; run;

data clean.groupe_cied_jul;
	set clean.groupe_cied_jul;
	if not missing(date_index_ccam) and not missing(date_deces_dcir_pmsi)
and date_index_ccam = date_deces_dcir_pmsi then deces_avant_index_ccam = 1; /*Date ccam il est décédé...*/
	run;

data clean.groupe_sub_cied_jul;
	set clean.groupe_cied_jul;
	where deces_avant_index_ccam = . and top_defi_ccam = 1; run;














