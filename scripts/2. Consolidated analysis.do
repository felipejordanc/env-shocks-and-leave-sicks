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
forvalues i=6/42{
	gen dum_`i' = 0
	replace dum_`i' = 1 if value == `i'
}
drop dum_21
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
eststo model1: xtreg y_1 dum* i.date i.cod_comuna i.reg_month, fe vce(cluster cod_comuna)
eststo model2: xtreg y_2 dum* i.date i.cod_comuna i.reg_month, fe vce(robust)
eststo model3: xtreg y_3 dum* i.date i.cod_comuna i.reg_month, fe vce(robust)
esttab model1 model2 model3 using "${tables}/y_tot.tex",replace nonumber booktabs collabels(none) star(* 0.10 ** 0.05 *** 0.01) varlabels(_cons "Constant") nogaps keep(dum*) b(%9.4f) se(%9.3f) stats(N Mean,fmt("%9.0fc" "%9.4fc")) mtitles("t+1" "t+2" "t+3") nonotes
