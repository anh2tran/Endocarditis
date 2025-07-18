/*1/ce macro aide a creer un table avec les exposes et les non-exposes a partir matched_set de librarie de stephane dans R
  2/ Avant de lancer ce macro, il faut lancer macro definir_evt parce que il est inclu dans ce macro
  3/ Il y a 2 versions de macro create_matched_set_sas: premiere version pour le groupe ATCD d'EI, et deuxieme version pour les autres groupes, parce que
	dans la deuxieme version on va identifer les non-exposes qui devernira un expose au cours du suivi...
	Parametre: path_file: votre fichier csv a partir de R
			   groupe: groupe concerne (exposition)
			   risk1,2,3: autre facteurs de risque, qui n'est pas de l'exposition*/

%macro create_matched_set_sas (path_file =,
							groupe=, risk1=, risk2=, risk3=);
PROC IMPORT DATAFILE=&path_file.
  OUT=work.matched_&groupe.
    DBMS=csv 
    REPLACE;
    GETNAMES=YES;
RUN;

data cohort.matched_&groupe (drop=var1 rename=(dom = dom_&groupe exposure = exposure_&groupe strata=strata_&groupe));
	set work.matched_&groupe;
	where not missing(id);
	run;

proc sort data=cohort.matched_&groupe; by id; run;
proc sort data=cohort.app_all_groups; by id; run;

data set_matched_full; /*recuperer les variables necessaires dans la table grande cohort.app_all_groupes*/
	merge cohort.matched_&groupe(in=a) cohort.app_all_groups(in=b
	keep= id top_conso_2008 top_conso_2009 top_conso_2010
top_conso_2011 top_conso_2012 top_conso_2013 top_conso_2014 top_conso_2015 top_conso_2016 top_conso_2017 top_conso_2018 top_conso_2019
top_conso_2020 top_conso_2021 top_conso_2022 top_conso_2023 ben_idt_ano date_deces_dcir_pmsi date_deces_pmsi deces_dcir_pmsi date_index_&risk1. date_index_&risk2. date_index_&risk3.);
	by id;
	if a;
	run;

data set_controls;
	set set_matched_full;
	where exposure_&groupe. = 0;
	run;

data set_controls;
	set set_controls;
	year_match = year(dom_&groupe.);
	run;

/*Inclure que les patients avec au moins une consommation dans le snds dans 2 ans avant la date'appariement*/
data matched_consommant_ou_pas; 
	set set_controls;
 format conso_year_n_1 32. conso_year_n_2 32.;

    /* Loop over the years */
    array top_conso_array{15} top_conso_2022 top_conso_2021 top_conso_2020 
                                   top_conso_2019 top_conso_2018 top_conso_2017 top_conso_2016 
                                   top_conso_2015 top_conso_2014 top_conso_2013 top_conso_2012 
                                   top_conso_2011 top_conso_2010 top_conso_2009 top_conso_2008;

    if 2010 <= year_match <= 2023 then do;
        conso_year_n_1 = top_conso_array{2024 - year_match};
        conso_year_n_2 = top_conso_array{2025 - year_match};
    end;
run;

data matched_consommant_ou_pas;
	set matched_consommant_ou_pas;
	top_conso_2ans_avant = 0;
	if conso_year_n_1 = 1 or conso_year_n_2 = 1 then top_conso_2ans_avant = 1;
	run;

data set_controls_consommant;
	set matched_consommant_ou_pas;
	where top_conso_2ans_avant = 1;
	run;


/*Inclure que les patients non-exposes a aucun facteur de risque avant la date d'appariement*/
data set_controls_consommant;
	set set_controls_consommant;
	format date_censored_to_expose date9.;
	if not missing(date_index_&risk1.) and date_index_&risk1. <= dom_&groupe. then exclu = 1;
	if not missing(date_index_&risk2.) and date_index_&risk2. <= dom_&groupe. then exclu = 1;
	if not missing(date_index_&risk3.) and date_index_&risk3. <= dom_&groupe. then exclu = 1;
	run;

data set_controls_final (keep=ben_idt_ano id exposure_&groupe. dom_&groupe. strata_&groupe. ben_idt_ano date_deces_dcir_pmsi date_deces_pmsi deces_dcir_pmsi);
	set set_controls_consommant;
	where exclu = .;
	run;

/*Tirer au sort jusau'a maximum 5 sujects non-exposes*/
proc sql;
  create table sorted_subjects as
  select *,
         rand("uniform", 280870) as random_order /* Random number for each subject in the stratum */
  from set_controls_final
  order by strata_&groupe., random_order; /* First by strata, then by random number */
quit;

data sorted_subjects;
	set sorted_subjects;
	by strata_&groupe.;
	if first.strata_&groupe. then order = 1;
	else order + 1;
	run;

proc sql;
create table selected_subjects as select distinct *
from sorted_subjects
where order <=5
group by strata_&groupe., order;
quit;

data matched_controls;
	set selected_subjects;
	run;

data matched_treated (keep=ben_idt_ano id exposure_&groupe. dom_&groupe. strata_&groupe. ben_idt_ano date_deces_dcir_pmsi date_deces_pmsi deces_dcir_pmsi);
	set set_matched_full;
	where exposure_&groupe. = 1;
	run;

data matching.matched_set_&groupe;
	set matched_treated matched_controls;
	run;


proc sql;
create table count_ctl_opt1 as select distinct *, (count(distinct id)-1) as num_ctl
from matching.matched_set_&groupe.
group by strata_&groupe.;
quit;	

proc sql;
create table count_ctl_strata as select distinct
strata_&groupe., num_ctl
from count_ctl_opt1
order by strata_&groupe.;
quit;

data tab_strat_0;
	set count_ctl_strata;
	zero_ctl = 1;
	where num_ctl = 0;
	run;

/*Definir date de l'evenement*/
data tab_expose;
	set matching.matched_set_&groupe.;
	where exposure_&groupe. = 1;
	run;

data tab_non_expose;
	set matching.matched_set_&groupe.;
	where exposure_&groupe. = 0;
	run;

%definir_evt(cohort=tab_expose, groupe=&groupe.);
%definir_evt(cohort=tab_non_expose, groupe=&groupe.);

proc sort data=tab_expose; by strata_&groupe.; run;
proc sort data=tab_strat_0; by strata_&groupe.; run;

data tab_expose;
	merge tab_expose (in=a) tab_strat_0 (in=b keep=strata_&groupe. zero_ctl);
	by strata_&groupe;
	if a;
	run;

data matching.matched_set_&groupe. (drop=order random_order);
	set tab_expose tab_non_expose;
	run;

/*identifier combien des exposes apparies avec 0,1,2,3,4,5 non-exposes*/
proc freq data=count_ctl_strata; table num_ctl/ out=table_N; run;

data table_N_output (drop=percent);
	retain variable num_ctl COUNT percentage; /*force variables in a desired order*/
	set table_N;
	length variable $ 20;
	variable = "&groupe.";
	percentage = put(percent, 6.2);
	run;

data matching.table_N_matched_sets;
	set matching.table_N_matched_sets table_N_output;
	run;

/*creer la table N sujets, N evt, incidence brut par 1000 personnes-annees*/
proc sql;
create table N_expose as select distinct count(distinct ben_idt_ano) as N_patient, sum(evenement) as N_evt, "expo_avant" as exposure length=32
from matching.matched_set_&groupe.
where exposure_&groupe. = 1;
quit;

proc sql;
create table N_expose_ctl as select distinct count(distinct ben_idt_ano) as N_patient, sum(evenement) as N_evt, "expo_apres" as exposure length=32
from matching.matched_set_&groupe.
where exposure_&groupe. = 1 and zero_ctl = .;
quit;

proc sql;
create table N_non_expose as select distinct count(distinct ben_idt_ano) as N_patient, sum(evenement) as N_evt, "nonexpo" as exposure
from matching.matched_set_&groupe.
where exposure_&groupe. = 0;
quit;

data N_pat_evt;
	set N_expose N_expose_ctl N_non_expose; run;

data N_pat_evt;
	set N_pat_evt;
	prevalence = put((N_evt/N_patient)*1000, 6.2);
	variable = "&groupe.";
	run;

data matching.N_pat_evt_matched_sets;
	retain variable exposure N_patient N_evt prevalence;
	set matching.N_pat_evt_matched_sets N_pat_evt;
	run;

data clean.matched_set_&groupe.;
	set matching.matched_set_&groupe.;
	run;

proc datasets library=work kill nolist;
quit;

%mend;


%macro create_matched_set_sas_2 (path_file =,
							groupe=, risk1=, risk2=, risk3=, risk4=);
PROC IMPORT DATAFILE=&path_file.
  OUT=work.matched_&groupe.
    DBMS=csv 
    REPLACE;
    GETNAMES=YES;
RUN;

data cohort.matched_&groupe (drop=var1 rename=(dom = dom_&groupe exposure = exposure_&groupe strata=strata_&groupe));
	set work.matched_&groupe;
	where not missing(id);
	run;

/*all matched sets currently have 10 controls matched with each treated subject, 
	in this step, we will select those controls with at least one record in the SNDS in the 2
	previous years, and then randomly select 5 controls among those remaining*/


proc sort data=cohort.matched_&groupe; by id; run;
proc sort data=cohort.app_all_groups; by id; run;

data set_matched_full;
	merge cohort.matched_&groupe(in=a) cohort.app_all_groups_1(in=b
	keep= id top_conso_2008 top_conso_2009 top_conso_2010
top_conso_2011 top_conso_2012 top_conso_2013 top_conso_2014 top_conso_2015 top_conso_2016 top_conso_2017 top_conso_2018 top_conso_2019
top_conso_2020 top_conso_2021 top_conso_2022 top_conso_2023 ben_idt_ano date_deces_dcir_pmsi date_deces_pmsi deces_dcir_pmsi
date_index_&risk1. date_index_&risk2. date_index_&risk3. date_index_&risk4. date_index_&groupe.);
	by id;
	if a;
	run;

data set_controls;
	set set_matched_full;
	where exposure_&groupe. = 0;
	run;

data set_controls;
	set set_controls;;
	year_match = year(dom_&groupe.);
	run;

data matched_consommant_ou_pas;
	set set_controls;
 format conso_year_n_1 32. conso_year_n_2 32.;

    /* Loop over the years */
    array top_conso_array{15} top_conso_2022 top_conso_2021 top_conso_2020 
                                   top_conso_2019 top_conso_2018 top_conso_2017 top_conso_2016 
                                   top_conso_2015 top_conso_2014 top_conso_2013 top_conso_2012 
                                   top_conso_2011 top_conso_2010 top_conso_2009 top_conso_2008;

    if 2010 <= year_match <= 2023 then do;
        conso_year_n_1 = top_conso_array{2024 - year_match};
        conso_year_n_2 = top_conso_array{2025 - year_match};
    end;
run;

data matched_consommant_ou_pas;
	set matched_consommant_ou_pas;
	top_conso_2ans_avant = 0;
	if conso_year_n_1 = 1 or conso_year_n_2 = 1 then top_conso_2ans_avant = 1;
	run;

data set_controls_consommant;
	set matched_consommant_ou_pas;
	where top_conso_2ans_avant = 1;
	run;

data set_controls_consommant;
	set set_controls_consommant;
	format date_censored_to_expose date9.;
	if not missing(date_index_&risk1.) and date_index_&risk1. <= dom_&groupe. then exclu = 1;
	if not missing(date_index_&risk2.) and date_index_&risk2. <= dom_&groupe. then exclu = 1;
	if not missing(date_index_&risk3.) and date_index_&risk3. <= dom_&groupe. then exclu = 1;
	if not missing(date_index_&risk4.) and date_index_&risk4. < dom_&groupe. then exclu = 1;
	if not missing(date_index_&groupe.) and date_index_&groupe. > dom_&groupe. then censored_to_expose = 1;
	if not missing(date_index_&groupe.) and date_index_&groupe. > dom_&groupe. then date_censored_to_expose = date_sej_index_&groupe.;
	run;

data set_controls_final (keep=ben_idt_ano id exposure_&groupe. dom_&groupe. strata_&groupe. date_deces_dcir_pmsi date_deces_pmsi deces_dcir_pmsi censored_to_expose date_censored_to_expose);
	set set_controls_consommant;
	where exclu = .;
	run;

proc sql;
  create table sorted_subjects as
  select *,
         rand("uniform", 280870) as random_order /* Random number for each subject in the stratum */
  from set_controls_final
  order by strata_&groupe., random_order; /* First by strata, then by random number */
quit;

data sorted_subjects;
	set sorted_subjects;
	by strata_&groupe.;
	if first.strata_&groupe. then order = 1;
	else order + 1;
	run;

proc sql;
create table selected_subjects as select distinct *
from sorted_subjects
where order <=5
group by strata_&groupe., order;
quit;

data matched_controls;
	set selected_subjects;
	run;

data matched_treated (keep=ben_idt_ano id exposure_&groupe. dom_&groupe. strata_&groupe. ben_idt_ano date_deces_dcir_pmsi date_deces_pmsi deces_dcir_pmsi);
	set set_matched_full;
	where exposure_&groupe. = 1;
	run;

data matching.matched_set_&groupe; /*matched controls before matched_treated as control dataset has more columns*/
	set matched_controls matched_treated;
	run;

/*Count number of controls in each stratum*/

/*option 1 - apply criteria at least one record in the SNDS in the 2 previous years*/
proc sql;
create table count_ctl_opt1 as select distinct *, (count(distinct id)-1) as num_ctl
from matching.matched_set_&groupe.
group by strata_&groupe.;
quit;	

proc sql;
create table count_ctl_strata as select distinct
strata_&groupe., num_ctl
from count_ctl_opt1
order by strata_&groupe.;
quit;

data tab_strat_0;
	set count_ctl_strata;
	zero_ctl = 1;
	where num_ctl = 0;
	run;

/*Definir date de l'evenement*/
data tab_expose;
	set matching.matched_set_&groupe.;
	where exposure_&groupe. = 1;
	run;

data tab_non_expose;
	set matching.matched_set_&groupe.;
	where exposure_&groupe. = 0;
	run;

%definir_evt(cohort=tab_expose, groupe=&groupe.);
%definir_evt(cohort=tab_non_expose, groupe=&groupe.);

proc sort data=tab_expose; by strata_&groupe.; run;
proc sort data=tab_strat_0; by strata_&groupe.; run;

data tab_expose;
	merge tab_expose (in=a) tab_strat_0 (in=b keep=strata_&groupe. zero_ctl);
	by strata_&groupe;
	if a;
	run;


data matching.matched_set_&groupe. (drop=order random_order);
	set tab_expose tab_non_expose;
	run;

proc freq data=count_ctl_strata; table num_ctl/ out=table_N; run;

data table_N_output (drop=percent);
	retain variable num_ctl COUNT percentage; /*force variables in a desired order*/
	set table_N;
	length variable $ 20;
	variable = "&groupe.";
	percentage = put(percent, 6.2);
	run;

data matching.table_N_matched_sets;
	set matching.table_N_matched_sets table_N_output;
	run;

proc sql;
create table N_expose as select distinct count(distinct ben_idt_ano) as N_patient, sum(evenement) as N_evt, "expose" as exposure
from matching.matched_set_&groupe.
where exposure_&groupe. = 1;
quit;

proc sql;
create table N_expose_ctl as select distinct count(distinct ben_idt_ano) as N_patient, sum(evenement) as N_evt, "exposectl" as exposure
from matching.matched_set_&groupe.
where exposure_&groupe. = 1 and zero_ctl = .;
quit;

proc sql;
create table N_non_expose as select distinct count(distinct ben_idt_ano) as N_patient, sum(evenement) as N_evt, "nonexpo" as exposure
from matching.matched_set_&groupe.
where exposure_&groupe. = 0;
quit;

data N_pat_evt;
	set N_expose N_expose_ctl N_non_expose; run;

data N_pat_evt;
	set N_pat_evt;
	prevalence = put((N_evt/N_patient)*1000, 6.2);
	variable = "&groupe.";
	run;

data matching.N_pat_evt_matched_sets;
	retain variable exposure N_patient N_evt prevalence;
	set matching.N_pat_evt_matched_sets N_pat_evt;
	run;

data clean.matched_set_&groupe.;
	set matching.matched_set_&groupe.;
	run;

proc datasets library=work kill nolist;
quit;

%mend;

/*create table to stock results*/
proc sql;
create table matching.table_N_matched_sets
(variable char(32),
num_ctl num(10),
COUNT num(10),
percentage char(32));
quit;

proc sql;
create table matching.N_pat_evt_matched_sets
(variable char(32),
exposure char(32),
N_patient num(10),
N_evt num(10),
prevalence char(32));
quit;

%create_matched_set_sas(path_file = "&Fichiers/REPEPIP/ETUDES/ENDOCARDITE/ANH/matched_atcd_1007.csv",
groupe=atcd,
risk1 = pv,
risk2 = mcm,
risk3 = cied)

%create_matched_set_sas_2(path_file = "&Fichiers/REPEPIP/ETUDES/ENDOCARDITE/ANH/matched_cied_ccam_1007.csv",
groupe=ccam_cied,
risk1 = atcd_ei,
risk2 = mcm,
risk3 = pv,
risk4 =cied)

%create_matched_set_sas_2(path_file = "&Fichiers/REPEPIP/ETUDES/ENDOCARDITE/ANH/matched_pv_ccam_1007.csv",
groupe=ccam_pv,
risk1 = atcd_ei,
risk2 = mcm,
risk3 = cied,
risk4 =pv)

%create_matched_set_sas_2(path_file = "&Fichiers/REPEPIP/ETUDES/ENDOCARDITE/ANH/matched_pv_1007.csv",
groupe=pv,
risk1 = atcd_ei,
risk2 = mcm,
risk3 = cied)

%create_matched_set_sas_2(path_file = "&Fichiers/REPEPIP/ETUDES/ENDOCARDITE/ANH/matched_pv_1007.csv",
groupe=pv,
risk1 = atcd_ei,
risk2 = mcm,
risk3 = cied)

%create_matched_set_sas_2(path_file = "&Fichiers/REPEPIP/ETUDES/ENDOCARDITE/ANH/matched_mcm_1007.csv",
groupe=mcm,
risk1 = atcd_ei,
risk2 = pv,
risk3 = cied)

%create_matched_set_sas_2(path_file = "&Fichiers/REPEPIP/ETUDES/ENDOCARDITE/ANH/matched_cied_1007.csv",
groupe=cied,
risk1 = atcd_ei,
risk2 = pv,
risk3 = mcm)
