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
reshape wide min_val max_val mean_val, i(Estación date) j(medida) string
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
keep if share >= 0.2
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
forvalues q=13/20{
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
	}
	collapse (sum) max_val* min_val* mean_val* (mean) Pers, by(comb Entidad cod_comuna date decile)	
	foreach m in MP10 MP25 NOX O3{
		replace min_val`m' = . if min_val`m'==0 & max_val`m'==0 & mean_val`m'==0
		replace max_val`m' = . if min_val`m'==. & max_val`m'==0 & mean_val`m'==0
		replace mean_val`m' = . if min_val`m'==. & max_val`m'==. & mean_val`m'==0
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
	}
	collapse (sum) max_val* min_val* mean_val*, by(date cod_comuna)	
	foreach m in MP10 MP25 NOX O3{
		replace min_val`m' = . if min_val`m'==0 & max_val`m'==0 & mean_val`m'==0
		replace max_val`m' = . if min_val`m'==. & max_val`m'==0 & mean_val`m'==0
		replace mean_val`m' = . if min_val`m'==. & max_val`m'==. & mean_val`m'==0
	}
	
	tempfile pol_`q'
	save `pol_`q''
}
use `pol_1', clear
forvalues q=2/21{
	append using `pol_`q''
}

order cod_comuna date min_valMP10 mean_valMP10 max_valMP10 min_valMP25 mean_valMP25 max_valMP25 min_valNOX mean_valNOX max_valNOX min_valO3 mean_valO3 max_valO3
sort cod_comuna date
foreach m in MP10 MP25 NOX O3{
	rename mean_val`m' mean`m'
	rename max_val`m' max`m'
	rename min_val`m' min`m'
}
save "$usedata/pollution_input.dta", replace























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
duplicates drop Entidad shp, force
destring Pers, replace
collapse (sum) Pers, by(cod_comuna)
rename Pers total_pers
tempfile com
save `com'
local i = 1
foreach m in MP25 MP10 NOX O3{
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
	keep if medida=="`m'"
	preserve
	keep Estación
	duplicates drop Estación, force
	tempfile cen
	save `cen'
	restore
	tempfile pol
	save `pol'
	foreach n in 1 2 3 4 5 6 7 8 9 10{
		import excel "$rawdata\pollution\entidades_centros.xlsx", firstrow clear
		destring Pers, replace
		drop if Estación=="." 
		merge m:1 Estación using `cen', nogenerate keep(3)
		bysort Entidad shp: egen total2 = total(weight)
		replace weight = weight/total
		drop total
		xtile decile = cod_comuna, nq(10)
		keep if decile == `n'
		cross using `dates'
		merge m:1 Estación date using `pol', nogenerate keep(3)

		replace min_val = min_val * weight
		replace max_val = max_val * weight
		replace mean_val = mean_val * weight
		collapse (sum) max_val min_val mean_val (mean) Pers, by(date cod_comuna Entidad shp)
		replace min_val = . if min_val==0 & max_val==0 & mean_val==0
		replace max_val = . if min_val==. & max_val==0 & mean_val==0
		replace mean_val = . if min_val==. & max_val==. & mean_val==0
		merge m:1 cod_comuna using `com', nogenerate keep(3)
		gen pob_w = Pers/total_pers
		replace mean_val = mean_val * pob_w
		replace max_val = max_val * pob_w
		replace min_val = min_val * pob_w
		collapse (sum) max_val min_val mean_val, by(date cod_comuna)
		replace min_val = . if min_val==0 & max_val==0 & mean_val==0
		replace max_val = . if min_val==. & max_val==0 & mean_val==0
		replace mean_val = . if min_val==. & max_val==. & mean_val==0
		gen medida="`m'"
		tempfile e_`i'
		save `e_`i''
		local ++i
	}
}
use `e_1'
forvalues y = 2/40{
	append using `e_`y''
}
sort cod_comuna medida date
rename max_val max
rename min_val min 
rename mean_val mean 
reshape wide max min mean, i(cod_comuna date) j(medida) string
merge n:1 cod_comuna using "$usedata/pollution_share.dta", keepusing(cod_comuna) keep(3) nogenerate
save "$usedata/pollution.dta", replace
