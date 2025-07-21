/*Groupe ATCD d'EI*/
data data_step1 (keep=ben_idt_ano ben_nir_psa DGN_PAL DGN_REL date_execution
exe_soi_dtd exe_soi_dtf an_pmsi RSA_NUM ETA_NUM); /*Enlever ben_rng_gem pour éviter les doublons des séjours*/
	set results.conso_pat_endo_cim; /*table source toutes les sejours avec DP/DR EI large liste*/
	run;

proc sql;
	create table data_step1 as select distinct * 
	from data_step1;
quit;
 
proc sql; /*Pour occupérer les variables ben_dcd_dte date_deces_pmsi ben_nai_ann*/
   create table data_step2 as 
   select distinct
      a.ben_idt_ano,
      a.ben_dcd_dte,
      a.date_deces_pmsi,
	  a.ben_nai_ann,
	  a.TOP_FICTIF,
	  a.TOP_JUMEAUX_MEME_SEXE,
	  a.MAX_EXE_SOI_DTF_ALL,
	  b.*
   from cohort.pop_general a
   inner join data_step1 b
      on a.ben_idt_ano = b.ben_idt_ano;
quit;

/*Identifier les séjours concernants aux jumeaux même sexe*/
data tab_jumeaux;
	set results.cohorte_jum_endo_cim;
	top_jumeaux_meme_sexe_sej = 1;
	run;

proc sort data=data_step2; by ben_nir_psa; run;
proc sort data=tab_jumeaux; by ben_nir_psa; run;

data data_step3;
	merge data_step2 (in=a) tab_jumeaux(in=b keep=ben_nir_psa top_jumeaux_meme_sexe_sej);
	if a;
	by ben_nir_psa;
	run;

/*Compter N*/
proc sql;
	create table work.table_N_1 as select count(distinct ben_idt_ano) as N_pat, "Total_pat" as step length=32
	from data_step3; 
	quit;

/*Enlever les sujets avec les NIR fictif et les séjours concernant les jumeaux même sexe*/
data data_step4a; 
	set data_step3;
	age_hospit = an_pmsi - ben_nai_ann;
	where TOP_FICTIF = 0;
	run;

data data_step4; 
	set data_step4a;
	age_hospit = an_pmsi - ben_nai_ann;
	where top_jumeaux_meme_sexe_sej = . and top_jumeaux_meme_sexe = 0; /*also use top_jumeaux_meme_sexe to exclude*/
	run;

/*Compter N apres NIR fictif et jumeaux*/
proc sql;
	create table work.table_N_2 as select count(distinct ben_idt_ano) as N_pat, "apres_fictif" as step length=32
	from data_step4;
	quit;

/*Enlever les sujets quand les patients < 18 ans*/
data data_step5 (drop=TOP_FICTIF top_jumeaux_meme_sexe);
	set data_step4;
	where age_hospit >= 18 and not missing (ben_nai_ann);
	run;

proc means data=data_step5 min max median; /*min=18 ans max = 106 ans ok*/
	variable age_hospit; run;

/*Compter N au moins 18 ans et annee de naissance pas manquante*/
proc sql;
	create table work.table_N_3 as select count(distinct ben_idt_ano) as N_pat, "18ans" as step length=32
	from data_step5; quit;

/*Fusionner les séjours hospitalisers, utilisant ben_idt_ano comme unique identifiant*/
proc sort data=data_step5;
by ben_idt_ano exe_soi_dtd;
run;


data data_step6;
	set data_step5;
	by ben_idt_ano;
	format fusion_exe_soi_dtd fusion_exe_soi_dtf date9.;
	retain fusion_exe_soi_dtd fusion_exe_soi_dtf;
	if first.ben_idt_ano then do;
		fusion_exe_soi_dtd = exe_soi_dtd;
		fusion_exe_soi_dtf = exe_soi_dtf;
	end;
		else do;
	if exe_soi_dtd <= fusion_exe_soi_dtf + 7 then fusion_exe_soi_dtf = exe_soi_dtf;
	else do;
	fusion_exe_soi_dtd = exe_soi_dtd;
	fusion_exe_soi_dtf = exe_soi_dtf;
	end;
	end;
	run;

/*table avec les séjours fusionnés avec diagnostiques*/
proc sql;
create table data_step7 as select distinct 
	ben_idt_ano,
	ETA_NUM,
	RSA_NUM,
	DGN_PAL,
	DGN_REL,
	exe_soi_dtd,
	exe_soi_dtf,
	ben_dcd_dte,
	date_deces_pmsi,
	fusion_exe_soi_dtd as fusion_sejour_start format=date9.,
	max(fusion_exe_soi_dtf) as fusion_sejour_end format=date9.
from data_step6
group by ben_idt_ano, fusion_exe_soi_dtd;
quit;

data table_avec_DPDR;
	set data_step7;
	where exe_soi_dtd = fusion_sejour_start;
	run;

PROC sql; 
create table cohort.sejours_fusion_2904 as select distinct
ben_idt_ano,
fusion_sejour_start,
fusion_sejour_end,
ben_dcd_dte,
date_deces_pmsi,
year(fusion_sejour_end) as an_pmsi_sortie,
(fusion_sejour_end - fusion_sejour_start) as nombre_jour_sejour
from data_step7; quit;


proc sql;
	create table work.table_N_4 as select count(distinct ben_idt_ano) as N_pat, "fusionner" as step length=32
	from cohort.sejours_fusion_2904; quit;

/*CHERCHER LES SEJOURS INDEX*/
/*Premier sejour hospitalier pendant la période 01/01/2010 - 30/06/2023 ( à la base de date_sortie) pour avoir 
3 mois de grace période et 3 mois du suivi au moins*/
data data_step8; 
set cohort.sejours_fusion_2904;
where fusion_sejour_end <= "30JUN2023"d; /*date sortie sejour index doit etre avant 30 Jun 2023 pour assurer 3 mois du suivi*/
run;

proc sql; /*Compter N*/
	create table work.table_N_5 as select count(distinct ben_idt_ano) as N_pat, "premier_sej" as step length=32
	from data_step8;
	quit;


data tab_vivant;
	set data_step8;
	where missing(date_deces_pmsi) or (fusion_sejour_end < date_deces_pmsi);
	run;

/*compter N patients avec une sortie vivant*/
proc sql;
	create table work.table_N_6 as select count(distinct ben_idt_ano) as N_pat, "sortie_vivant" as step length=32
	from tab_vivant;
	quit;

/*Durée des séjours*/
data tab_vivant_7jour; 
	set tab_vivant;
	where nombre_jour_sejour >=7;
	run;

proc sql;
	create table work.table_N_7 as select count(distinct ben_idt_ano) as N_pat, "duree_7j" as step length=32
	from tab_vivant_7jour;
	quit;

proc sort data=tab_vivant_7jour; by ben_idt_ano; run;

data tab_count_sejour;
	set tab_vivant_7jour;
	by ben_idt_ano;
	if first.ben_idt_ano then count_sejour = 1;
	else count_sejour + 1;
	run;

data tab_premier_sejour;
	set tab_count_sejour;
	where count_sejour = 1;
	run;

proc sql;
	create table work.N_no_premier as select count(*) as N_no_premier
	from tab_count_sejour
	where count_sejour > 1; quit;

proc sql;
	create table work.N_premier_sejour as select count(*) as N_premier_sejour
	from tab_premier_sejour;
	quit;

proc sql;
	create table work.N_premier_sejour_patient as select count(distinct ben_idt_ano) as N_premier_sejour
	from tab_premier_sejour;
	quit;

%macro merge_data();
%do i=2006 %to 2023;
%let year=&i;
data tab_premier_sejour;
	merge tab_premier_sejour (in=a) 
cohort.pop_general (in=b keep=ben_idt_ano top_conso_dcir_&year top_conso_pmsi_&year);
by ben_idt_ano;
if a;
run;
%end;
%mend merge_data;

%merge_data;

%macro creat_merge_data_conso();
%do i=2006 %to 2023;
%let year=&i;
data tab_premier_sejour;
	set tab_premier_sejour;
	top_conso_&year = 0;
	if top_conso_dcir_&year = 1 or top_conso_pmsi_&year = 1 then top_conso_&year = 1;
run;
%end;
%mend creat_merge_data_conso;

%creat_merge_data_conso;

data cohort.full_set_expose;
	set tab_premier_sejour;
	run;


data conso_2ans_av;
    set cohort.full_set_expose;
    format conso_year_n_1 32. conso_year_n_2 32.;

    /* Loop over the years */
    array top_conso_array{15} top_conso_2022 top_conso_2021 top_conso_2020 
                                   top_conso_2019 top_conso_2018 top_conso_2017 top_conso_2016 
                                   top_conso_2015 top_conso_2014 top_conso_2013 top_conso_2012 
                                   top_conso_2011 top_conso_2010 top_conso_2009 top_conso_2008;

    if 2010 <= an_pmsi_sortie <= 2023 then do;
        conso_year_n_1 = top_conso_array{2024 - an_pmsi_sortie};
        conso_year_n_2 = top_conso_array{2025 - an_pmsi_sortie};
    end;

run;

data conso_2ans_av;
	set conso_2ans_av;
	top_conso_2ans_avant = 0;
	if conso_year_n_1 = 1 or conso_year_n_2 = 1 then top_conso_2ans_avant = 1;
	run;

proc sql;
	create table work.N_no_conso as select count(*) as N_no_conso
	from conso_2ans_av
	where top_conso_2ans_avant = 0;
	quit;

data cohort.groupe_atcd_ei_1 (keep=ben_idt_ano date_deces_pmsi ben_dcd_dte
fusion_sejour_start fusion_sejour_end nombre_jour_sejour an_pmsi_sortie date_index MAX_EXE_SOI_DTF_ALL);
	set conso_2ans_av;
	format date_index date9.;
	where top_conso_2ans_avant = 1;
	date_index = fusion_sejour_end + 90;
	run; 

proc sql;
create table work.table_N_8 as select count(distinct ben_idt_ano) as N_pat, "conso" as step length=32
from cohort.groupe_atcd_ei_1;
quit;

/*IDENTIFIER LA DATE DE DECES BASEE SUR DCIR ET PMSI*/
%let cohort_entree = cohort.groupe_atcd_ei_1; /*assurer qu'il y a 3 variables: ben_dcd_dte, date_deces_pmsi, et max_exe_soi_dtf_all*/

/*Delai entre date deces pmsi and max date consommation*/
data &cohort_entree.;
	merge &cohort_entree. (in=a) cohort.pop_general (in=b keep=ben_idt_ano max_exe_soi_dtf_all);
	by ben_idt_ano;
	if a;
	run;

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

data set3_deces; 
	set set2_deces; 
	if deces_dcir_pmsi = 1 and missing(date_deces_dcir_pmsi) then missing_date_deces = 1; 
	if deces_dcir_pmsi = 1 and not missing(date_deces_dcir_pmsi) and date_deces_dcir_pmsi <= date_index then deces_avant_index = 1; run;
run;

data clean.groupe_atcd_ei_jul;
	set set3_deces;
	where deces_avant_index = . and missing_date_deces = .; run; /*no deces avant index and no missing date deces*/

proc sql;
	create table work.table_N_9 as select count(distinct ben_idt_ano) as N_pat, "final" as step
	from clean.groupe_atcd_ei_jul; quit;

/*IDENTIFIER EVENEMENT*/
%definir_evt(cohort=clean.groupe_atcd_ei_jul);

data N_flowchart;
	set table_n_1 table_n_2 table_n_3 table_n_4
	table_n_5 table_n_6 table_n_7 table_n_8 table_n_9;
	run;


