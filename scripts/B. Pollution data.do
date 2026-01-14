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
*       1. Random sample                       *
************************************************
import delimited "$rawdata\pollution\neighbors_centros.csv", varnames(1) clear
replace entidad = entidad + shp*1000000
forvalues i=1/5{
	gen neig_`i' = ///
    real(substr(close_`i', 1, strpos(close_`i', "-")-1)) + ///
    real(substr(close_`i', strpos(close_`i', "-")+1, .))*1000000
	drop close_`i'
}
drop shp cod_comuna
save "$usedata/neighbors.dta", replace


import delimited "$rawdata\pollution\Data_Pollution_cleaned.csv", varnames(1) clear
gen date = daily(fechayymmdd, "YMD")
format date %td
drop fechayymmdd
keep if medida == "Material_particulado_MP_10" | medida == "Material_particulado_MP_25" | medida == "Ozono-" | medida == "xidos_de_nitrgeno"
replace medida = "MP10" if medida == "Material_particulado_MP_10"
replace medida = "MP25" if medida == "Material_particulado_MP_25"
replace medida = "NOX" if medida == "xidos_de_nitrgeno"
replace medida = "O3"  if medida == "Ozono-"
rename centro Estación
replace min_val = . if min_val==0 & max_val==0 & mean_val==0
replace max_val = . if min_val==. & max_val==0 & mean_val==0
replace mean_val = . if min_val==. & max_val==. & mean_val==0
replace median_val = . if min_val==. & max_val==. & mean_val==.

reshape wide min_val max_val mean_val median_val, i(Estación date) j(medida) string
save "$usedata/pollution_data.dta", replace

import excel "$rawdata\pollution\entidades_centros.xlsx", firstrow clear
duplicates drop Entidad shp, force
destring Pers, replace
collapse (sum) Pers, by(cod_comuna)
rename Pers total_pers
save "$usedata/ent_com.dta", replace

import excel "$rawdata\pollution\entidades_centros.xlsx", firstrow clear
preserve
destring Pers, replace
gen suma = Pers if Estación != "."
collapse (sum) Pers suma, by(cod_comuna)
gen share = suma/Pers
keep if share >= 0.8
keep cod_comuna share
save "$usedata/pollution_share.dta", replace
restore
replace Entidad = Entidad + shp*1000000
bysort Entidad (Estación): gen n = _n
drop if Estación=="." 
keep Entidad Estación n
reshape wide Estación, i(Entidad) j(n)
keep if Estación2 != ""
gen comb = ""

forvalues i = 1/7 {
    replace comb = cond(comb=="", Estación`i', comb + " - " + Estación`i') if Estación`i' != ""
}
duplicates drop comb, force
reshape long Estación, i(comb) j(n)
keep if Estación != ""
drop Entidad 
save "$usedata/pollution_comb.dta", replace

clear all
set obs 1
gen date = mdy(1,1,2018)
format date %td
gen enddate = mdy(12,31,2024)
format enddate %td
expand enddate - date + 1
replace date = date[1] + _n - 1
drop enddate
tempfile dates
save `dates'
use "$usedata/pollution_comb.dta", clear
cross using `dates'
sort comb Estación date
merge n:1 Estación date using "$usedata/pollution_data.dta", keep (1 3) nogenerate
save "$usedata/pollution_comb_data.dta", replace



clear all
set obs 1
gen date = mdy(1,1,2018)
format date %td
gen enddate = mdy(12,31,2024)
format enddate %td
expand enddate - date + 1
replace date = date[1] + _n - 1
drop enddate
tempfile dates
save `dates'
import excel "$rawdata\pollution\entidades_centros.xlsx", firstrow clear
replace Entidad = Entidad + shp*1000000
bysort Entidad (Estación): gen countid = _n
drop if Estación=="." 
drop shp
reshape wide Estación weight, i(Entidad cod_comuna Pers) j(countid)
gen comb = ""

forvalues i = 1/7 {
    replace comb = cond(comb=="", Estación`i', comb + " - " + Estación`i') if Estación`i' != ""
}
reshape long Estación weight, i( Entidad cod_comuna Pers comb) j(n)
keep if Estación != ""
xtile decile = cod_comuna, nq(20)
forvalues q=1/20{
	preserve
	keep if decile == `q'
	cross using `dates'
	destring Pers, replace
	merge n:1 Estación date using "$usedata/pollution_data.dta", keep (1 3) nogenerate
	rename n countid
	merge n:1 comb countid date using "$usedata/pollution_comb_data_R.dta", keep (1 3 4 5) nogenerate update 
	compress
	foreach m in MP10 MP25 NOX O3{
		gen weight_aux_`m' = weight if max_val`m' != .
		bysort Entidad date: egen weight_`m' = total(weight_aux_`m')
		replace weight_`m' = weight/weight_`m'
		drop weight_aux_`m'
	}
	foreach m in MP10 MP25 NOX O3{
		replace min_val`m' = min_val`m' * weight_`m'
		replace max_val`m' = max_val`m' * weight_`m'
		replace mean_val`m' = mean_val`m' * weight_`m'
		replace median_val`m' = median_val`m' * weight_`m'
	}
	collapse (sum) max_val* min_val* mean_val* median_val* (mean) Pers, by(comb Entidad cod_comuna date decile)	
	foreach m in MP10 MP25 NOX O3{
		replace min_val`m' = . if min_val`m'==0 & max_val`m'==0 & mean_val`m'==0
		replace max_val`m' = . if min_val`m'==. & max_val`m'==0 & mean_val`m'==0
		replace mean_val`m' = . if min_val`m'==. & max_val`m'==. & mean_val`m'==0
		replace median_val`m' = . if min_val`m'==. & max_val`m'==. & mean_val`m'==.
	}
	save "$usedata/pollution_comuna_`q'.dta", replace
	restore
}
forvalues q=1/20{
	use "$usedata/pollution_comuna_`q'.dta", clear
	compress
	egen tag = tag(cod_comuna comb)
	bys cod_comuna: egen distinct_comb = total(tag)
	preserve
	keep if distinct_comb == 1
	drop tag distinct_comb
	tempfile pol_`q'
	save `pol_`q''
	restore
	keep if distinct_comb > 1
	drop tag distinct_comb
	save "$usedata/pollution_comuna_`q'.dta", replace

}
use `pol_1', clear
forvalues q=2/20{
	append using `pol_`q''
}
compress
save "$usedata/pollution_comuna_21.dta", replace

forvalues q=1/21{
	use "$usedata/pollution_comuna_`q'.dta", clear
	merge m:1 cod_comuna using "$usedata/ent_com.dta", nogenerate keep(3)
	gen pob_w = Pers/total_pers
	
	foreach m in MP10 MP25 NOX O3{
		replace min_val`m' = min_val`m' * pob_w
		replace max_val`m' = max_val`m' * pob_w
		replace mean_val`m' = mean_val`m' * pob_w
		replace median_val`m' = median_val`m' * pob_w
	}
	collapse (sum) max_val* min_val* mean_val* median_val*, by(date cod_comuna)	
	foreach m in MP10 MP25 NOX O3{
		replace min_val`m' = . if min_val`m'==0 & max_val`m'==0 & mean_val`m'==0
		replace max_val`m' = . if min_val`m'==. & max_val`m'==0 & mean_val`m'==0
		replace mean_val`m' = . if min_val`m'==. & max_val`m'==. & mean_val`m'==0
		replace median_val`m' = . if min_val`m'==. & max_val`m'==. & mean_val`m'==.
	}
	
	tempfile pol_`q'
	save `pol_`q''
}
use `pol_1', clear
forvalues q=2/21{
	append using `pol_`q''
}

order cod_comuna date min_valMP10 mean_valMP10 max_valMP10 median_valMP10 min_valMP25 mean_valMP25 max_valMP25 median_valMP25 min_valNOX mean_valNOX max_valNOX median_valNOX min_valO3 mean_valO3 max_valO3 median_valO3
sort cod_comuna date
foreach m in MP10 MP25 NOX O3{
	rename mean_val`m' mean`m'
	rename max_val`m' max`m'
	rename min_val`m' min`m'
	rename median_val`m' median`m'
}
merge n:1 cod_comuna using "$usedata/pollution_share.dta", nogenerate keep(3)
save "$usedata/pollution_input.dta", replace



************************************************
*       1. Pollution reg db               *
************************************************






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
compress
gen year = year(date)
merge n:1 benef_enciptado year using "$usedata/id_com.dta", keep(1 3) nogenerate
foreach m in MP10 MP25 NOX O3{
	preserve
	merge n:1 cod_comuna date using "$usedata/pollution_input.dta", keep(1 3) nogenerate keepusing(mean`m' maxMP25 maxMP10 maxNOX maxO3  median`m')
	gen week = wofd(date)
	format week %tw
	drop id_all year cod_comuna
	compress
	gen mean`m'_max = mean`m'
	gen max`m'_mean = max`m'
	gen MP10ex_max = maxMP10 >130 if maxMP10 != .
	gen MP25ex_max = maxMP25 >50 if maxMP25 != .
	gen NOXex_max = maxNOX >100 if maxNOX != .
	gen O3ex_max = maxO3 >120 if maxO3 != .
	gen suma = 1
	gen median`m'_max = median`m'
	collapse (max) median`m'_max mean`m'_max max`m' (sum) suma  `m'ex_max  (mean) max`m'_mean  mean`m'  median`m' , by(benef_enciptado week)

	compress
	rename week date
	replace `m'ex_max = `m'ex_max/suma
	drop suma
	tempfile e_`m'
	save `e_`m''
	restore
}
use `e_MP10', clear
merge 1:1 benef_enciptado date using `e_MP25', nogenerate 
merge 1:1 benef_enciptado date using `e_NOX', nogenerate 
merge 1:1 benef_enciptado date using `e_O3', nogenerate 
foreach m in MP10 MP25 NOX O3{
	replace `m'ex_max = . if max`m' == .
}
save "$usedata/pollution_merge.dta", replace
















