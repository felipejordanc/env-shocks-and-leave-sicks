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
eststo model1: reghdfe y_1 dum*, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model2: reghdfe y_2 dum*, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model3: reghdfe y_3 dum*, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
esttab model1 model2 model3 using "${tables}/y_tot.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(dum*) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("t+1" "t+2" "t+3") nonotes
estimates clear
eststo model1: reghdfe y_1 dum* if codigo_empleador == 1, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model2: reghdfe y_2 dum* if codigo_empleador == 1, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model3: reghdfe y_3 dum* if codigo_empleador == 1, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
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
keep if id_all == 1
set seed 12345
*sample 80
cross using `dates'
sort benef_enciptado date
merge 1:1 benef_enciptado date using "$usedata/Sick2.dta", keepusing(cod_tipo_licencia) nogenerate keep(1 3)
replace cod_tipo_licencia = cod_tipo_licencia !=.

save "$usedata/temp.dta", replace
use "$usedata/temp.dta", clear
gen year = year(date)
merge n:1 benef_enciptado year using "$usedata/id_com.dta", keep(1 3) nogenerate
merge n:1 cod_comuna date using "$usedata/climate_tmax.dta", keep(1 3) nogenerate
replace value = round(value)
gen dum_9 = value <10
gen dum_38 = value >37

forvalues i=5/18{
	gen dum_`=`i'*2'_`=`i'*2+1' = 0
	replace dum_`=`i'*2'_`=`i'*2+1' = 1 if value == `i'*2 | value == `i'*2 + 1
}
drop dum_20_21
forvalues i=0/3{
	gen y_`i' = 0
	bysort benef_enciptado (date): replace y_`i' = 1 if cod_tipo_licencia[_n+`i'] == 1
}
gen month = month(dofm(date))
gen reg = floor(cod_comuna/1000)
gen reg_month= month*100 + reg
compress
save "$usedata/reg_w.dta", replace
use "$usedata/reg_w.dta", clear
drop if year == 2020 | year == 2021
drop id_all cod_tipo_licencia year month reg y_1 y_2 y_3 value
xtset benef_enciptado date
gen y_1 = y_0
forvalues k = 1/7 {
    bys benef_enciptado (date): replace y_1 = 1 if F`k'.y_0 == 1
}
replace y_0 = 100 if y_0 == 1
replace y_1 = 100 if y_1 == 1


drop dum*
merge n:1 cod_comuna date using "$usedata/pollution.dta", keep(1 3) nogenerate keepusing(meanMP10 meanMP25 maxMP10 maxMP25)

eststo model0p: reghdfe y_1 meanMP10, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model1p: reghdfe y_1 meanMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model2p: reghdfe y_1 meanMP10 meanMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)

esttab model0p model1p model2p using "${tables}/y_sick1_pol.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(mean*) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("t+7" "t+7" "t+7") nonotes

eststo model3p: reghdfe y_0 meanMP10, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model4p: reghdfe y_0 meanMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model5p: reghdfe y_0 meanMP10 meanMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)

esttab model3p model4p model5p using "${tables}/y_sick0_pol.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(mean*) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("t" "t" "t") nonotes
eststo model6p: reghdfe y_1 maxMP10, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model7p: reghdfe y_1 maxMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model8p: reghdfe y_1 maxMP10 maxMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)

esttab model6p model7p model8p using "${tables}/y_sick1_polm.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(max*) b(%9.4f) se(%9.3f) stats(N max,fmt("%9.0fc" "%9.4fc")) mtitles("t+7" "t+7" "t+7") nonotes

eststo model9p: reghdfe y_0 maxMP10, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model10p: reghdfe y_0 maxMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model11p: reghdfe y_0 maxMP10 maxMP25, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)

esttab model9p model10p model11p using "${tables}/y_sick0_polm.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(max*) b(%9.4f) se(%9.3f) stats(N max,fmt("%9.0fc" "%9.4fc")) mtitles("t" "t" "t") nonotes

esttab model* using "${tables}/y_sick0_pol_tot.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(max*) b(%9.5f) se(%9.4f) stats(N max,fmt("%9.0fc" "%9.4fc")) mtitles("t+7" "t+7" "t+7" "t" "t" "t" "t+7" "t+7" "t+7" "t" "t" "t") nonotes

use "$usedata/reg_w.dta", clear

drop if year == 2020 | year == 2021
drop id_all cod_tipo_licencia year month reg y_1 y_2 y_3 value
xtset benef_enciptado date
gen y_1 = y_0
forvalues k = 1/7 {
    bys benef_enciptado (date): replace y_1 = 1 if F`k'.y_0 == 1
}
replace y_0 = 100 if y_0 == 1
replace y_1 = 100 if y_1 == 1

eststo model0p: reghdfe y_1 dum*, absorb(date cod_comuna reg_month) vce(cluster cod_comuna)
eststo model1p: reghdfe y_1 dum*, absorb(date cod_comuna reg_month benef_enciptado) vce(cluster cod_comuna)

esttab model0p model1p using "${tables}/y_sick1.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(mean*) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("t+7" "t+7,i") nonotes