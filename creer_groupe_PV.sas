/*cohorte Porteurs de prothèse Atran 10/07*/
/*Joindre les cohortes construites à partir cim et ccam*/
proc sql;
create table conso_pv_ccam as select distinct
a.ben_idt_ano,
a.date_execution as date_execution_ccam,
a.date_execution as date_entree_cohorte format=date9., /*date execution = date index corrigé 10/07*/
year(a.date_execution) as annee_execution,
b.ben_nai_ann,
b.TOP_FICTIF,
b.TOP_JUMEAUX_MEME_SEXE
from cohort.conso_pv_ccam a, cohort.pop_general b
where a.ben_idt_ano = b.ben_idt_ano and a.date_execution <= "30JUN2023"d;
quit;

proc sql;
create table conso_pv_cim as select distinct
a.ben_idt_ano,
a.date_execution as date_execution_cim,
a.exe_soi_dtf as date_entree_cohorte format=date9.,/*Parce que pour les séjours hospitliers, on compte a partir de date de sortie*/
year(a.exe_soi_dtf) as annee_execution,
b.ben_nai_ann,
b.TOP_FICTIF,
b.TOP_JUMEAUX_MEME_SEXE
from cohort.conso_pv_cim a, cohort.pop_general b
where a.ben_idt_ano = b.ben_idt_ano and a.exe_soi_dtf <= "30JUN2023"d; /*date sortie avant*/
quit;

data conso_ccam;
	set conso_pv_ccam;
	where TOP_FICTIF = 0 and TOP_JUMEAUX_MEME_SEXE = 0;
	run;

data conso_cim;
	set conso_pv_cim;
	where TOP_FICTIF = 0 and TOP_JUMEAUX_MEME_SEXE = 0;
	run;

proc sql;
	create table table_N_1 as select count(distinct ben_idt_ano) as N_pat, "pv_ccam_total" as step length=32
	from conso_ccam;
	quit;

proc sql;
	create table table_N_2 as select count(distinct ben_idt_ano) as N_pat, "pv_cim_total" as step length=32
	from conso_cim;
	quit;

/*Restreindre aux patients >= 18 ans*/
data conso_pv_ccam_adulte;
	set conso_ccam;
	age_exe = annee_execution - ben_nai_ann;
	run;

data conso_pv_cim_adulte;
	set conso_cim;
	age_exe = annee_execution - ben_nai_ann;
	run;

proc sql;
create table tab_top_pv_ccam as select distinct
ben_idt_ano,
min(date_entree_cohorte) as date_entree_cohorte_ccam format=date9.,
min(date_execution_ccam) as date_execution_min_ccam format=date9.,
1 as top_pv_ccam
from conso_pv_ccam_adulte
where age_exe >=18 and ben_nai_ann ne "0000"
group by ben_idt_ano;
quit;


proc sql;
create table tab_top_pv_cim as select distinct
ben_idt_ano,
min(date_entree_cohorte) as date_entree_cohorte_cim format=date9., /*calculé à partir de date sortie*/
min(date_execution_cim) as date_execution_min_cim format=date9.,
1 as top_pv_cim
from conso_pv_cim_adulte
where age_exe >= 18 and ben_nai_ann ne "0000"
group by ben_idt_ano;
quit;

/*Compte N >=18 ans*/
proc sql;
create table table_N_3 as select count(distinct ben_idt_ano) as N_pat, "pv_ccam_18ans" as step length=32
from tab_top_pv_ccam;
quit;

proc sql;
create table table_n_4 as select count(distinct ben_idt_ano) as N_pat, "pv_cim_18ans" as step length=32
from tab_top_pv_cim;
quit;

/*Merger 2 cohortes ccam et cim*/
data tab_pv_1;
	merge cohort.pop_general (in=a) tab_top_pv_ccam(in=b);
	if a;
	by ben_idt_ano;
	run;

data tab_pv_2;
	merge tab_pv_1 (in=a) tab_top_pv_cim(in=b);
	if a;
	by ben_idt_ano;
	run;

data tab_pv;
	set tab_pv_2;
	where top_pv_ccam = 1 or top_pv_cim =1;
	run;

/*Compte N de cohorte mergé*/
proc sql;
create table table_N_5 as select count(distinct ben_idt_ano) as N_pat, "N_total" as step length=32
from tab_pv;
quit;

data tab_pv_temps; /*definir date index*/
	set tab_pv;
	format date_entree_cohorte date9.;
	if missing(top_pv_cim) then date_entree_cohorte = date_entree_cohorte_ccam;
	if missing(top_pv_ccam) then date_entree_cohorte = date_entree_cohorte_cim;
	if top_pv_ccam = 1 and top_pv_cim = 1 then 
	date_entree_cohorte = min(date_entree_cohorte_ccam, date_entree_cohorte_cim);
	run;

/*date execution final = date execution min for ccam, date sortie min for cim-10*/

data tab_pv_temps_1;
	set tab_pv_temps;
	no_ccam_exact =.;
	if missing(top_pv_ccam) then no_ccam_exact = 1;
	if top_pv_cim = 1 and top_pv_ccam = 1 and date_execution_min_cim < date_execution_min_ccam 
then no_ccam_exact = 1;
	run;

data tab;
set tab_pv_temps_1;
class_ccam= 0; /*only ccam*/
class_cim=0; /*only cim*/
class_ccam_before = 0; /*ccam before cim*/
class_cim_before=0; /*cim before ccam*/
if missing(top_pv_cim) and top_pv_ccam=1 then class_ccam = 1;;
if missing(top_pv_ccam) and top_pv_cim=1 then class_cim =1;
if top_pv_cim = 1 and top_pv_ccam = 1 and date_execution_min_cim < date_execution_min_ccam then class_cim_before = 1;; 
if top_pv_cim = 1 and top_pv_ccam = 1 and date_execution_min_cim >= date_execution_min_ccam then class_ccam_before = 1; 
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

data tab_pv_temps_1; set tab_pv_temps_1; annee_execution = year(date_entree_cohorte); where ben_nai_ann ne "0000"; run;

%macro creat_merge_data_conso();
%do i=2006 %to 2023;
%let year=&i;
data tab_pv_temps_1;
	set tab_pv_temps_1;
	top_conso_&year = 0;
	if top_conso_dcir_&year = 1 or top_conso_pmsi_&year = 1 then top_conso_&year = 1;
run;
%end;
%mend creat_merge_data_conso;

%creat_merge_data_conso;


data conso_2ans_av;
    set tab_pv_temps_1;
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
create table cohort.pv_july as select distinct
ben_idt_ano,
date_entree_cohorte as date_index format=date9., /*date entree cohort = date execution= date index*/
date_execution_min_ccam,
date_execution_min_cim,
date_entree_cohorte_ccam,
date_entree_cohorte_cim,
top_pv_ccam,
top_pv_cim,
no_ccam_exact,
date_deces_pmsi,
ben_dcd_dte,
MAX_EXE_SOI_DTF_ALL,
1 as top_defi_pacemaker
from conso_2ans_av
where top_conso_2ans_avant = 1;
quit;

/*Compte N cohort porteurs de prothese*/
proc sql;
create table table_n_6 as select count(distinct ben_idt_ano) as N_PAT, "conso" as step length=32
from cohort.pv_july; quit;

data work.set1; /*Je juste voudrais toujours assurer que toutes les codes ne touchent pas ma pricipale cohorte*/
	set cohort.pv_july;
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
if deces_dcir_pmsi = 1 and not missing(date_deces_dcir_pmsi) and date_deces_dcir_pmsi <= date_index then deces_avant_index = 1; run; /*meme si date de sortie ou date d'execution = date index mais juste pour assurer*/
run;

/*il y a quand meme les patients sortie decede a la fin du sejour CIM #PV ou lors procedure #PV*/
data clean.groupe_pv_jul;
	set set3_deces;
	where deces_avant_index = . and missing_date_deces = .; run; /*no deces avant index and no missing date deces*/


proc sql;
	create table table_N_7 as select count(distinct ben_idt_ano) as N_pat, "final" as step length=32
	from clean.groupe_pv_jul; quit;


data N_flowchart;
	set table_n_1 table_n_2 table_n_3 table_n_4
	table_n_5 table_n_6 table_n_7;
	run;


/******CONSTITUTION DES SOUS-GROUPES*******/
data set1 (keep=ben_idt_ano date_index date_deces_dcir_pmsi);
	set clean.groupe_pv_jul;
	run;

proc sql;
create table ccam_pv as select distinct
a.*,
b.date_index
from cohort.conso_ccam_pv as a
inner join set1 as b
        on a.ben_idt_ano = b.ben_idt_ano and
		a.date_execution >= b.date_index;
quit;

proc sort data=ccam_pv; by ben_idt_ano date_execution; run;

data ccam_pv_2;
	set ccam_pv;
	by ben_idt_ano;
	if first.ben_idt_ano then order_ccam = 1;
	else order_ccam + 1;
	run;

data first_ccam_pv (rename=(date_execution = date_index_ccam));
	set ccam_pv_2;
	where order_ccam = 1;
	run;

data first_ccam_pv;
	set first_ccam_pv;
	top_index_tavi = 0;
	top_index_mitra = 0;
	top_index_cec = 0;
	if  ACTE_CCAM in ("DBLF001") then top_index_tavi = 1;
if  ACTE_CCAM in ("DBBF198") then top_index_mitra = 1;
if ACTE_CCAM in ("DBKA006",
"DBMA009",
"DBMA003",
"DBKA011",
"DGKA015",
"DBKA010",
"DBMA008",
"DGKA011",
"DBMA007",
"DBMA013",
"DBKA009",
"DBKA002",
"DGKA014",
"DBMA006",
"DBKA003",
"DBKA004",
"DBMA004",
"DGKA018",
"DBMA001",
"DBKA012",
"DBEA001",
"DBKA001",
"DBMA015",
"DBMA005",
"DBMA010",
"DBKA005",
"DBKA008",
"DGKA698",
"DGKA263") then top_index_cec = 1;
run;

/*uniquement un type de CCAM*/
data ccam_pv;
	set ccam_pv;
	count_not_tavi = 1;
	count_not_mitra = 1;
	count_not_cec = 1;
	if  ACTE_CCAM in ("DBLF001") then count_not_tavi = 0;
if  ACTE_CCAM in ("DBBF198") then count_not_mitra = 0;
if ACTE_CCAM in ("DBKA006",
"DBMA009",
"DBMA003",
"DBKA011",
"DGKA015",
"DBKA010",
"DBMA008",
"DGKA011",
"DBMA007",
"DBMA013",
"DBKA009",
"DBKA002",
"DGKA014",
"DBMA006",
"DBKA003",
"DBKA004",
"DBMA004",
"DGKA018",
"DBMA001",
"DBKA012",
"DBEA001",
"DBKA001",
"DBMA015",
"DBMA005",
"DBMA010",
"DBKA005",
"DBKA008",
"DGKA698",
"DGKA263") then count_not_cec = 0; run;

proc sql;
create table top_subgroup_pv_b as select distinct
ben_idt_ano,
date_index,
sum(count_not_tavi) as top_not_tavi_b,
sum(count_not_mitra) as top_not_mitra_b,
sum(count_not_cec) as top_not_cec_b
from ccam_pv
group by ben_idt_ano;
quit;

data top_subgroup_pv (drop=top_not_tavi_b top_not_mitra_b top_not_cec_b);
	set top_subgroup_pv_b;
	top_not_tavi=top_not_tavi_b;
	top_not_mitra = top_not_mitra_b;
	top_not_cec = top_not_cec_b;
	if top_not_tavi_b > 1 then top_not_tavi = 1;
	if top_not_mitra_b > 1 then top_not_mitra = 1;
	if top_not_cec_b > 1 then top_not_cec = 1;
	run;

data top_subgroup_pv (drop=top_not_tavi top_not_mitra top_not_cec);
	set top_subgroup_pv;
	top_tavi_unique = 1;
	top_mitra_unique = 1;
	top_cec_unique = 1;
	if top_not_tavi = 1 then top_tavi_unique = 0;
	if top_not_mitra = 1 then top_mitra_unique = 0;
	if top_not_cec = 1 then top_cec_unique = 0;
	run;

proc sort data=top_subgroup_pv; by ben_idt_ano; run;
proc sort data=first_ccam_pv; by ben_idt_ano;

data clean.subgroup_pv_july;
	merge top_subgroup_pv (in=a) first_ccam_pv (in=a keep=ben_idt_ano 
top_index_tavi top_index_mitra top_index_cec date_index_ccam);
	by ben_idt_ano;
	if a;
	run;

proc freq data=clean.subgroup_pv_july; 
table top_tavi_unique top_mitra_unique top_cec_unique top_index_tavi top_index_mitra top_index_cec; run;

proc sort data=clean.groupe_pv_jul; by ben_idt_ano; run;
proc sort data=clean.subgroup_pv_july; by ben_idt_ano; run;

data clean.groupe_pv_jul;
	merge clean.groupe_pv_jul (in=a) clean.subgroup_pv_july (in=b keep=ben_idt_ano top_tavi_unique top_mitra_unique top_cec_unique
top_index_tavi top_index_mitra top_index_cec date_index_ccam);
by ben_idt_ano; 
if a; run;

data clean.groupe_pv_jul;
	set clean.groupe_pv_jul;
	if not missing(date_index_ccam) and not missing(date_deces_dcir_pmsi)
and date_index_ccam = date_deces_dcir_pmsi then deces_avant_index_ccam = 1; /*Date ccam il est décédé...*/
	run;

proc freq data=clean.groupe_pv_jul; table deces_avant_index_ccam; run;

data clean.groupe_sub_pv_jul; /*184 patients decedes lors procedures*/
	set clean.groupe_pv_jul;
	where deces_avant_index_ccam = . and top_pv_ccam = 1;
	run;


