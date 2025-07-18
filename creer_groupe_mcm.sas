/* MCM cardiaques:  code CIM-10, NEW VERSION 09 JULY 2025*/
/*Pour les sejours, travaillez sur ben_idt_ano + BEN_NIR_PSA*/

/*création liste code CIM, CCAM et LPP*/
%let Fichiers=%sysget(HOME)/sasdata;

libname icd "&Fichiers/REPEPIP/ETUDES/ANH_EI/list";

libname RESULTS "&Fichiers/REPEPIP/ETUDES/ANH_EI/results";


/*Défibrillateurs et stimulateurs cardiaques :codes CIM-10 dans PMSI*/
data icd.cim_MCM;
input code_cim $6.;
datalines;
I424
Q20
Q200
Q203
Q204
Q205 
Q210
Q211
Q212
Q213
Q218
Q219
Q234
;
run;

%let dat_debut_glob=%sysfunc(datetime());
%include "%sysget(HOME)/sasdata/REPPHARE/OUTILS/MACROS/DONNEES/recherche_patients_pathologie_V2.sas";
%recherche_patients_pathologie_V2(Date_debut=01/01/2010,
Date_fin=31/12/2023,
Ref_patient=epipop_20250320_psa,
Recherche=CIM_SEJOURS,
Option=MCO,
Option_CIM_MCO=SEJ_DPDR_DAS ,
Option_CIM_HAD=,
Option_CIM_SSR=,
Option_CIM_PSY=,
ALD_FIN=OUI,
Secteur_sanitaire=NON,
PMSI_mensuel=NON,
lib_resultat=results,
moi_liq_sup=6,
indicateur=malfo,
liste=icd.cim_mcm,
liste2=,
Initiation=NON,
Ini_Tps_Sans=0,
Nb_Conso=1,
dates_distinctes =,
conserver_donnees=OUI,
verif_param_uniquement=NON);

/*Chercher les sejours avec cim-10 codes valides et date sortie avant 30/06/2023*/

/*proc sql;
create table cohort.sejour_mcm_sans_Q210 as select distinct
ben_idt_ano,
ben_nir_psa,
sej_nbj,
an_pmsi,
date_execution,
exe_soi_dtd,
exe_soi_dtf,
DGN_REL,
DGN_PAL,
ASS_DGN
from results.conso_pat_malfo
where DGN_PAL in ("Q200", "Q203", "Q204", "Q205", "Q212", "Q213", "Q220")
	or DGN_REL in ("Q200", "Q203", "Q204", "Q205", "Q212", "Q213", "Q220")
	or ASS_DGN in ("Q200", "Q203", "Q204", "Q205", "Q212", "Q213", "Q220");
	quit;

data cohort.sejour_mcm_inclu_sans_Q210;
	set cohort.sejour_mcm_sans_Q210;
	where exe_soi_dtf <= "30JUN2023"d;
	run;*/

/*flowchart AVEC Q210*/
proc sql; /*Pour occupérer les variables ben_dcd_dte date_deces_pmsi ben_nai_ann*/
   create table data_step1 as 
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
   inner join cohort.sejour_mcm_inclu b
      on a.ben_idt_ano = b.ben_idt_ano;
quit;

/*Identifier les séjours concernants aux jumeaux même sexe*/
data tab_jumeaux;
	set results.cohorte_jum_malfo;
	top_jumeaux_meme_sexe_sej = 1;
	run;

proc sort data=data_step1; by ben_nir_psa; run;
proc sort data=tab_jumeaux; by ben_nir_psa; run;

data data_step2;
	merge data_step1 (in=a) tab_jumeaux(in=b keep=ben_nir_psa top_jumeaux_meme_sexe_sej);
	if a;
	by ben_nir_psa;
	run;


/*Enlever les sujets avec les NIR fictif et les séjours concernant les jumeaux même sexe*/
data data_step3a; 
	set data_step2;
	annee_debut = ben_nai_ann + 18; /*suivi a partir de 18 ans*/
	where TOP_FICTIF = 0; 
	run;

data data_step3; 
	set data_step3a;
	where top_jumeaux_meme_sexe_sej = . and top_jumeaux_meme_sexe = 0; /*also use top_jumeaux_meme_sexe to exclude*/;
	run;

proc sql;
	create table work.table_n_1 as select count(distinct ben_idt_ano) as N_pat, "total" as step length=32
	from data_step3; 
	quit;

/*Enlever les sujets quand les patients sans l'annee de naissance and patients pas ayant 18 ans jusqu'en 2023*/
data data_step4 (drop=TOP_FICTIF top_jumeaux_meme_sexe);
	set data_step3;
	format date_debut date9.;
	date_debut = mdy(1, 1, annee_debut);
	where not missing (ben_nai_ann) and annee_debut <= 2023;
	run;

data data_step5; /*date index sera debut de l'annee de 18 ans ou 01/01/2010, qui est plus tard*/
	set data_step4;
	format date_index date9.;
	if annee_debut >= 2010 and annee_debut <= 2023 then date_index=date_debut;
	if annee_debut < 2010 then date_index= "1JAN2010"d;
	run;

data data_step5;
	set data_step5;
	an_pmsi = year(date_index);
	run;

proc sort data=data_step5; by ben_idt_ano; run;
proc sort data=cohort.pop_general; by ben_idt_ano; run;

%macro merge_data();
%do i=2006 %to 2023;
%let year=&i;
data data_step5;
	merge data_step5 (in=a) 
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
data data_step5;
	set data_step5;
	top_conso_&year = 0;
	if top_conso_dcir_&year = 1 or top_conso_pmsi_&year = 1 then top_conso_&year = 1;
run;
%end;
%mend creat_merge_data_conso;

%creat_merge_data_conso;

data conso_2ans_av;
    set data_step5;
    format conso_year_n_1 32. conso_year_n_2 32.;

    /* Loop over the years */
    array top_conso_array{15} top_conso_2022 top_conso_2021 top_conso_2020 
                                   top_conso_2019 top_conso_2018 top_conso_2017 top_conso_2016 
                                   top_conso_2015 top_conso_2014 top_conso_2013 top_conso_2012 
                                   top_conso_2011 top_conso_2010 top_conso_2009 top_conso_2008;

    if 2010 <= an_pmsi <= 2023 then do;
        conso_year_n_1 = top_conso_array{2024 - an_pmsi};
        conso_year_n_2 = top_conso_array{2025 - an_pmsi};
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

data cohort.cohort_mcm_1 (keep=ben_idt_ano date_deces_pmsi ben_dcd_dte
date_execution exe_soi_dtf an_pmsi date_index MAX_EXE_SOI_DTF_ALL);
	set conso_2ans_av;
	where top_conso_2ans_avant = 1;
	run; 

/*pour faire exactement la meme procedure avec les autres groupes, j'ai fait top conso avant date deces.. mais honnetement 
	perso je trouve plus logique avec date deces avant top conso, resultats sont pareils donc..*/

proc sql;
create table table_n_2 as select count(distinct ben_idt_ano) as N_pat, "conso" as step length=32
from cohort.cohort_mcm_1;
quit;

/*IDENTIFIER LA DATE DE DECES BASEE SUR DCIR ET PMSI*/
%let cohort_entree = cohort.cohort_mcm_1; /*assurer qu'il y a 3 variables: ben_dcd_dte, date_deces_pmsi, et max_exe_soi_dtf_all*/

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

data clean.groupe_mcm_jul;
	set set3_deces;
	where deces_avant_index = . and missing_date_deces = .; run; /*no deces avant index and no missing date deces*/

proc sql;
create table table_n_3 as select count(distinct ben_idt_ano) as N_pat, "final" as step length=32 from clean.groupe_mcm_jul; quit;

data table_flowchart;
	set table_n_1 table_n_2 table_n_3;
	run;




