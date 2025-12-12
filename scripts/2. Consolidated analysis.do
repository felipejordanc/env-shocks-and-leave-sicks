*********************************************
* Purpose: Sick leave database
*
* Date: 05/08/2025
*
* Author: Matías Black
*********************************************
clear all

************************************************
*                0. Key Macros                 *
************************************************

*Folder globals

di "current user: `c(username)'"
if "`c(username)'" == "black"{
	global path "C:/Users/black/Documents/GitHub/env-shocks-and-leave-sicks"
}



	global rawdata "$path/remote/data/A_raw"
	global usedata "$path/remote/data/B_intermediate"
	global final "$path/remote/data/C_final"
	global graphs "$path/graphs"
	global tables "$path/tables"
	
************************************************
*       1. Regressions                 *
************************************************
set maxvar 32000
use "$usedata/climate_tmax.dta", clear
rename date date_td
gen date = mofd(date_td)
format date %tm
collapse (max) value, by(cod_comuna date)
replace value = round(value)
save "$usedata/tmax_reg.dta", replace
use "$usedata/climate_tmax.dta", clear
collapse (max) value, by(cod_comuna date)
replace value = round(value)
save "$usedata/tmax_w.dta", replace
clear all
set obs 1
gen start = ym(2018, 1)
gen end   = ym(2024,12)
format start %tm
format end   %tm
expand end - start + 1
replace start = start[1] + _n - 1
rename start date
drop end
format date %tm
tempfile dates
save `dates'

use "$usedata/benef_sample1.dta", clear
cross using `dates'
sort benef_enciptado date
merge 1:1 benef_enciptado date using "$usedata/Employer.dta", keepusing(codigo_empleador) nogenerate
replace codigo_empleador = codigo_empleador !=.
gen year = year(dofm(date))
merge n:1 benef_enciptado year using "$usedata/id_com.dta", keep(1 3) nogenerate
merge n:1 cod_comuna date using "$usedata/tmax_reg.dta", keep(1 3) nogenerate
gen dum_9 = value <10
gen dum_38 = value >37

forvalues i=5/18{
	gen dum_`=`i'*2'_`=`i'*2+1' = 0
	replace dum_`=`i'*2'_`=`i'*2+1' = 1 if value == `i'*2 | value == `i'*2 + 1
}
drop dum_20_21
forvalues i=1/3{
	gen y_`i' = 0
	bysort benef_enciptado (date): replace y_`i' = 1 if codigo_empleador[_n+`i'] == 1
}
gen month = month(dofm(date))
gen reg = floor(cod_comuna/1000)
gen reg_month= month*100 + reg
save "$usedata/reg.dta", replace
use "$usedata/reg.dta", clear
xtset benef_enciptado date
forvalues i=1/3{
	replace y_`i' = 100 if y_`i' == 1
}

estimates clear
compress
eststo model1: reghdfe y_1 dum* if codigo_empleador == 1, absorb(date cod_comuna reg_month benef_enciptado) vce(cluster cod_comuna)
eststo model2: reghdfe y_2 dum* if codigo_empleador == 1, absorb(date cod_comuna reg_month benef_enciptado) vce(cluster cod_comuna)
eststo model3: reghdfe y_3 dum* if codigo_empleador == 1, absorb(date cod_comuna reg_month benef_enciptado) vce(cluster cod_comuna)
esttab model1 model2 model3 using "${tables}/y_tot_ cond.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(dum*) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("t+1" "t+2" "t+3") nonotes


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

use "$usedata/sample_id.dta", clear
set seed 12345
*sample 80
cross using `dates'
sort benef_enciptado date
merge 1:1 benef_enciptado date using "$usedata/Sick.dta", keepusing(cod_tipo_licencia dias_otorgados delta) nogenerate keep(1 3)
replace cod_tipo_licencia = cod_tipo_licencia !=.
save "$usedata/temp.dta", replace
use "$usedata/temp.dta", clear
rename cod_tipo_licencia leave
gen enddate = .
replace enddate = date + dias_otorgados - 1 if dias_otorgados != .
bysort benef_enciptado (date): replace enddate = enddate[_n-1] if enddate[_n-1] > date[_n-1] & enddate[_n-1] != . & enddate == .
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
merge n:1 cod_comuna date using "$usedata/climate_tmax.dta", keep(1 3) nogenerate
merge n:1 cod_comuna date using "$usedata/pollution_input.dta", keep(1 3) nogenerate keepusing(meanMP10 meanMP25 maxMP10 maxMP25)
replace value = round(value)
gen leave2 = leave
gen value2 = value
gen week = wofd(date)
format week %tw
collapse (max) value2 leave2 delta cod_comuna maxMP10 maxMP25 (sum) leave cond (mean) value meanMP10 meanMP25, by(benef_enciptado week)
rename week date
gen month = month(dofw(date))
gen reg = floor(cod_comuna/1000)
gen reg_month= month*100 + reg
rename value2 max_temp
rename value mean_temp
rename leave2 y_dum
replace leave = 7 if leave >7
replace leave = leave/7
rename leave y_spn
gen year = year(dofw(date))
drop month reg
compress
save "$usedata/reg_w.dta", replace
use "$usedata/reg_w.dta", clear
gen dum_9 = max_temp <10
gen dum_38 = max_temp >37

forvalues i=5/18{
	gen dum_`=`i'*2'_`=`i'*2+1' = 0
	replace dum_`=`i'*2'_`=`i'*2+1' = 1 if max_temp == `i'*2 | max_temp == `i'*2 + 1
}
drop dum_20_21
drop if year == 2020 | year == 2021
xtset benef_enciptado date
replace y_dum = . if cond == 0
replace y_spn = . if cond == 0
replace y_dum = y_dum*100
replace y_spn = y_spn*100
estimates clear
eststo model0p: reghdfe y_dum dum*, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model1p: reghdfe y_spn dum*, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)

esttab model* using "${tables}/y_sick_w.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(dum*) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("Dummy" "Spain") nonotes
estimates clear

eststo model2p: reghdfe y_dum max_temp, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model3p: reghdfe y_dum mean_value, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model4p: reghdfe y_spn max_temp, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model5p: reghdfe y_spn mean_value, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)

esttab model* using "${tables}/y_sick_w_v.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps  b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("Dummy" "Dummy" "Spain" "Spain") nonotes

drop dum*
estimates clear
eststo model0p: reghdfe  y_dum meanMP10, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model1p: reghdfe  y_dum meanMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model2p: reghdfe  y_dum meanMP10 meanMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)



eststo model6p: reghdfe  y_dum maxMP10, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model7p: reghdfe  y_dum maxMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model8p: reghdfe  y_dum maxMP10 maxMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)



eststo model3p: reghdfe  y_spn meanMP10, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model4p: reghdfe  y_spn meanMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model5p: reghdfe  y_spn meanMP10 meanMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)



eststo model9p: reghdfe  y_spn maxMP10, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model10p: reghdfe  y_spn maxMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model11p: reghdfe  y_spn maxMP10 maxMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)



esttab model* using "${tables}/y_sick0_pol_tot.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(max* mean*) b(%9.5f) se(%9.4f) stats(N max,fmt("%9.0fc" "%9.4fc")) mtitles("Dummy" "Dummy" "Dummy" "Dummy" "Dummy" "Dummy" "Spain" "Spain" "Spain" "Spain" "Spain" "Spain") nonotes
estimates clear

gen pmex = maxMP10 >130 if maxMP10 != .



eststo model6p: reghdfe  y_dum pmex, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)


eststo model9p: reghdfe  y_spn pmex, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
gen pmex2 = maxMP25 >50 if maxMP25 != .

eststo model1p: reghdfe  y_dum pmex2, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)


eststo model2p: reghdfe  y_spn pmex2, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)




esttab model* using "${tables}/y_sick0_pol_tot_fe.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(pmex*) b(%9.5f) se(%9.4f) stats(N max,fmt("%9.0fc" "%9.4fc")) mtitles("Dummy" "Dummy" "Dummy" "Dummy" "Dummy" "Dummy" "Spain" "Spain" "Spain" "Spain" "Spain" "Spain") nonotes
estimates clear

forvalues i=1/20{
	gen dum_`=(`i'-1)*20+1'_`=`i'*20' = 0
	replace dum_`=(`i'-1)*20+1'_`=`i'*20' = 1 if maxMP10 >= `=(`i'-1)*20+1' & maxMP10 <= `=`i'*20'
}
estimates clear
gen dum_400 = maxMP10 > 400
drop dum_1_20
eststo model2p: reghdfe  y_dum dum*, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)




esttab model* using "${tables}/y_sick0_pol_tot_dum.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(dum*) b(%9.5f) se(%9.4f) stats(N max,fmt("%9.0fc" "%9.4fc")) mtitles("Dummy" "Dummy" "Dummy" "Dummy" "Dummy" "Dummy" "Spain" "Spain" "Spain" "Spain" "Spain" "Spain") nonotes
replace leave = leave*100


estimates clear
eststo model0p: reghdfe leave2 dum*, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model1p: reghdfe leave dum*, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)

esttab model* using "${tables}/y_sick1.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(dum*) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("t+7" "t+7,i") nonotes
estimates clear
merge n:1 benef_enciptado using "$usedata/benef_pct.dta", keep(1 3) nogenerate
keep if pct_id == 1 | pct_id== 2
sort benef_enciptado date
eststo model0p: reghdfe leave2 value L(1/6).value, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model1p: reghdfe leave value L(1/6).value, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
esttab model0p model1p using "${tables}/y_sickv.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(value) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("t+7" "t+7,i") nonotes