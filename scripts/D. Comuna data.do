************************************************
*       1. Random sample                       *
************************************************
foreach y in 2018 2019 2020{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	keep benef_enciptado cod_comuna_pernat 
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}


foreach y in 2022 2023 2024{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	rename código_beneficiario benef_enciptado
	keep benef_enciptado comuna_beneficiario
	merge n:1 comuna_beneficiario using "C:\Users\black\Dropbox\GSL\Bases intermedias\codigo_comunas.dta", update nogenerate keep(3)
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}
import delimited "$rawdata/population/Benef_FNS_2021\Benef_FNS_2021.txt", clear varnames(1)
rename id_asegurado benef_enciptado
merge n:1 comuna using "C:\Users\black\Dropbox\codigo_comunas.dta", update nogenerate keep(3)
keep benef_enciptado cod_comuna_pernat
tempfile b_2021
save `b_2021'

use `b_2018'
foreach y in 2019 2020 2021 2022 2023 2024{
	append using `b_`y''
}
drop comuna_beneficiario
replace year = 2021 if year == .
rename cod_comuna_pernat cod_comuna
merge n:1 cod_comuna using "$usedata/3rds_graph_com.dta", nogenerate
keep benef_enciptado tmax_3rd year
reshape wide tmax_3rd, i(benef_enciptado) j(year)
egen row_min = rowmin(tmax_3rd2018-tmax_3rd2024)
egen row_max = rowmax(tmax_3rd2018-tmax_3rd2024)
egen n_nonmiss = rownonmiss(tmax_3rd2018-tmax_3rd2024)

gen comtype = row_min if row_min == row_max & n_nonmiss == 7
drop row_min row_max tmax* n_nonmiss
drop if comtype == .
merge 1:1 benef_enciptado using "$usedata/benef_sample.dta", nogenerate keep(3)
sample 100000, count by(comtype)
save "$usedata/benef_temp.dta", replace


clear all



import delimited "$rawdata/sick leaves\T8314.csv", clear varnames(1)
rename encripbi_rut_traba benef_enciptado
merge n:1 benef_enciptado using "$usedata/benef_temp.dta", nogenerate keep(3)
gen date = daily(fecha_emision, "DMY")
format date %td
gen date2 = daily(fecha_desde, "DMY")
format date2 %td
gen year = year(date)
bysort rut_prof_encriptado year: gen obs_id_year = _N
bysort year: egen cutoff = pctile(obs_id_year), p(99)
gen top1pct = obs_id_year >= cutoff
gen delta = date - date2
keep if delta <= 7 & delta >= -7
gen resp = res_cod_capitulo_cie10 == "J00-J99"
preserve
duplicates drop benef_enciptado date, force // Cambiar a keep max
keep benef_enciptado date dias_otorgados cod_tipo_licencia top1pct delta lic_cod_previsional res_categoria_cie10 resp
gen reactive = delta>=0
label define deltal 1 "Reactive" 0 "Anticipated"
label values reactive deltal
keep if dias_otorgados <30
save "$usedata/Sick_com.dta", replace




clear all
set obs 1
gen date = mdy(1,1,2018)
format date %td
gen enddate = mdy(12,31,2024)
format enddate %td
expand enddate - date + 1
replace date = date[1] + _n +-1
drop enddate
tempfile dates
save `dates'

use "$usedata/benef_temp.dta", clear
set seed 12345
*sample 80
cross using `dates'
sort benef_enciptado date
merge 1:1 benef_enciptado date using "$usedata/Sick_com.dta", keepusing(cod_tipo_licencia dias_otorgados delta reactive resp) nogenerate keep(1 3)
replace cod_tipo_licencia = cod_tipo_licencia !=.
compress
rename cod_tipo_licencia leave
gen enddate = .
replace enddate = date + dias_otorgados - 1 if dias_otorgados != .
bysort benef_enciptado (date): replace enddate = enddate[_n-1] if enddate[_n-1] > date[_n-1] & enddate[_n-1] != . & enddate == .
gen leave2 = leave
replace leave2 = . if enddate != . & leave != 1
replace leave = 1 if enddate != . & leave != 1


drop enddate dias_otorgados
rename date date_td
gen date = mofd(date_td)
format date %tm
merge n:1 benef_enciptado date using "$usedata/benef_sampleyear_cond.dta", keep(1 3) 
gen cond = _merge == 3
drop _merge pct_id date
rename date_td date
gen year = year(date)
merge n:1 benef_enciptado year using "$usedata/id_com.dta", keep(1 3) nogenerate
gen leave3 = leave
gen leave_re = leave if reactive ==1
gen leave_ant = leave  if reactive ==0
replace leave2 = leave2 == .
gen leave_resp = leave if resp ==1

gen week = wofd(date)
format week %tw
drop delta year reactive
gen suma = 1
compress
collapse (max) leave3 leave_re leave_ant leave_resp cod_comuna (sum) suma leave leave2 cond (mean) comtype, by(benef_enciptado week)
rename week date
gen month = month(dofw(date))
gen reg = floor(cod_comuna/1000)
gen reg_month= month*100 + reg
rename leave3 y_dum
replace cond = 0 if cod_comuna == .
replace y_dum = . if cond == 0
replace y_dum = . if leave2 == suma
replace y_dum = 0 if leave2 == leave & leave <suma & cond != 0



replace leave = leave/suma
replace leave = y_dum if y_dum != 1
replace leave_re = y_dum if leave_re == . & y_dum != 1
replace leave_ant = y_dum if leave_ant == . & y_dum != 1
replace leave_re = 0 if leave_re == . & y_dum == 1
replace leave_ant = 0 if leave_ant == . & y_dum == 1
replace leave_resp = y_dum if leave_resp == . & y_dum != 1
replace leave_resp = 0 if leave_resp == . & y_dum == 1

rename leave y_spn
rename leave_re y_re
rename leave_ant y_ant
rename leave_resp y_resp
replace y_dum = y_dum*100
replace y_spn = y_spn*100
replace y_re = y_re*100
replace y_resp = y_resp*100
replace y_ant = y_ant*100
gen year = year(dofw(date))
drop month reg suma leave2 cond
compress
save "$usedata/reg_com.dta", replace














use "$usedata/reg_com.dta", clear
drop if year == 2020 | year == 2021
drop y_re-y_ant y_spn year
xtset benef_enciptado date
merge n:1 benef_enciptado date using "$usedata/tmax_dum2.dta", keep(1 3) nogenerate keepusing(tmax_dum*)



egen tmax_dum_10 = rowtotal(tmax_dum_10_11 tmax_dum_9)
egen tmax_dum_30 = rowtotal(tmax_dum_30_31 tmax_dum_32_33 tmax_dum_34)
drop tmax_dum_30_31 tmax_dum_32_33 tmax_dum_34 tmax_dum_10_11 tmax_dum_9
eststo model1p: reghdfe y_resp tmax_dum* if comtype == 1, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)



eststo model2p: reghdfe y_resp tmax_dum* if comtype == 2, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)



eststo model3p: reghdfe y_resp tmax_dum* if comtype == 3, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
esttab model* using "${tables}/income_tmax_comresp.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(tmax_dum*) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("Low Income_Cold") nonotes