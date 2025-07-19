%macro definir_evt(cohort=, groupe=); /*cohort = cohorte d'entree, groupe= groupe concerne, date d'appariement sous le format dom_&groupe.*/
proc sql;
create table tab_EI_event as select distinct
a.*
from cohort.tab_DPDR_EI a, &cohort. b /*cohort.tab_DPDR_EI est une table de sejours avec DPDR EI I330 deja nettoyee dans mon dossier cohort*/
where a.ben_idt_ano = b.ben_idt_ano and a.fusion_sejour_start > b.dom_&groupe.; /*dom_&groupe. = date index ou date de l'appariement*/
quit;

data tab_EI_event_possible;
	set tab_EI_event;
	deces = 0;
	if fusion_sejour_end = date_deces_pmsi then deces = 1; /*identifier les sejours EI avec une sortie deces*/
	run;

data tab_EI_event_possible;
	set tab_EI_event_possible;
	EI_possible = 0;
	if deces = 1 then EI_possible = 1; /*identifier les sejours EI avec une sortie deces*/
	if deces = 0 and nombre_jour_sejour >= 7 then EI_possible = 1; /*identifier les sejours EI avec une sortie vivant, durre >= 7 jours*/
	run;

data tab_EI_event_possible; 
	set tab_EI_event_possible;
	where EI_possible = 1; run;

proc sort data=tab_EI_event_possible; by ben_idt_ano fusion_sejour_start; run;

data tab_EI_sorted;
	set tab_EI_event_possible;
	by ben_idt_ano;
	if first.ben_idt_ano then count_hospit = 1;
	else count_hospit + 1;
	run;

data tab_EI_event (drop=count_hospit rename=(EI_possible=Evenement deces=deces_sejour_ei fusion_sejour_start = date_evenement));
	set tab_EI_sorted;
	where count_hospit = 1; /*identifer le premier sejour depuis le debut de la periode du suivi*/
	run;

proc sort data=tab_EI_event; by ben_idt_ano; run;
proc sort data=&cohort.; by ben_idt_ano; run;

data set1_evenement;
	merge &cohort. (in=a) tab_EI_event (in=b keep=ben_idt_ano deces_sejour_ei evenement date_evenement);
	by ben_idt_ano;
	if a;
	run;

data set2_evenement;
	set set1_evenement;
	deces_avant_fin_etude = .;
	censure_fin_etude = .;
	if evenement = . and deces_dcir_pmsi = 1 and date_deces_dcir_pmsi <= "31DEC2023"d then deces_avant_fin_etude = 1;
	if evenement = . and deces_dcir_pmsi = 1 and date_deces_dcir_pmsi > "31DEC2023"d then censure_fin_etude = 1;
	if evenement = . and deces_dcir_pmsi = 0 then censure_fin_etude = 1;
	run;

data set3_evenement;
	set set2_evenement;
	Endpoint = .;
	if Evenement = 1 then Endpoint = 1; /*Outcome of interest: new episode of IE*/
	If deces_avant_fin_etude = 1 then Endpoint = 2; /*Death*/
	If censure_fin_etude = 1 then Endpoint = 3; /*Censored at the the end of the study*/
	run;

data &cohort. (drop=evenement);
	set set3_evenement;
	format date_endpoint date9.;
	if Endpoint = 1 then date_endpoint = date_evenement;
	If Endpoint = 2 then date_endpoint = date_deces_dcir_pmsi;
	if Endpoint = 3 then date_endpoint = "31DEC2023"d;
	run;

data &cohort.;
	set &cohort.;
	if Endpoint = 1 then evenement = 1;
	if Endpoint ne 1 then evenement = 0;
	run;
%mend;
