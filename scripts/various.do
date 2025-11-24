foreach y in 2018 2019 2020{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	keep benef_enciptado
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}


foreach y in 2022 2023 2024{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	rename código_beneficiario benef_enciptado
	keep benef_enciptado
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}
import delimited "$rawdata/population/Benef_FNS_2021\Benef_FNS_2021.txt", clear varnames(1)
rename id_asegurado benef_enciptado
gen year = 2021
keep benef_enciptado year
tempfile b_2021
save `b_2021'

use `b_2018'
foreach y in 2019 2020 2021 2022 2023 2024{
	append using `b_`y''
}
duplicates drop benef_enciptado year, force
save "$usedata/benef_sample_pob.dta", replace

use "$usedata/benef_sampleyear.dta", clear //empleadores
compress
gen year = year(dofm(date))
gen month = month(dofm(date))
gen suma = 1
bysort benef_enciptado year (date): egen max = total(suma) if month > 5 & month != 12
drop month suma
collapse (mean) max, by(benef_enciptado year)
replace max = 0 if max == . | max <= 2
replace max = 1 if max >0
save "$usedata/benef_sampleyear2.dta", replace

use "$usedata/benef_sampleyear2.dta", clear
merge 1:1 benef_enciptado year using "$usedata/benef_sample_pob.dta", keep(1 3)
rename _merge match
fillin benef_enciptado year
replace max = . if max == 0
gen miss = 1 if max == . & year == 2018
bysort benef_enciptado (year): replace miss = 1 if miss[_n-1] == 1
drop _fillin miss match
reshape wide max, i(benef_enciptado) j(year)
egen total = rowtotal(max*) if max2018 != .
tab total


****** Sick leave population

import delimited "$rawdata/sick leaves\T8314.csv", clear varnames(1)
keep encripbi_rut_traba fecha_emision
rename encripbi_rut_traba benef_enciptado
gen date = daily(fecha_emision, "DMY")
format date %td
gen year = year(date)
duplicates drop benef_enciptado year, force
save "$usedata/benef_sample_sick.dta", replace



clear all
set obs 1
gen date = 2018
gen enddate = 2024
expand enddate - date + 1
replace date = date[1] + _n +-1
drop enddate
tempfile dates
save `dates'

use "$usedata/sample_id.dta", clear
keep if id_all == 1
cross using `dates'
rename date year
merge 1:1 benef_enciptado year using "$usedata/benef_sample_sick.dta", keep(1 3)
tab year _merge
replace date = date !=.
format date %9.0g
bysort benef_enciptado (year): egen total = total(date)
replace total = total > 0
tab total if year == 2018
rename _merge match1
merge 1:1 benef_enciptado year using "$usedata/id_com.dta", keep(1 3)
gen reg = floor(cod_comuna/1000)
tab reg total if year == 2018
tab cod_comuna total if year == 2018



local mydir "C:\Users\black\Documents\Plantillas personalizadas de Office\OneDrive_6_7-11-2025_0_100\LT8696_TEST_999_20250710\"
local files : dir "`mydir'" files "*.txt"
local i = 1
foreach f of local files {
	import delimited "`mydir'\`f'", varnames(1) clear
	rename codigo_cotizante benef_enciptado
	tostring periodo_renta, replace
	replace periodo_renta = substr(periodo_renta,1,4) + "m" + substr(periodo_renta,5,2)
	gen date = monthly(periodo_renta, "YM")
	format date %tm
	drop if date < tm(2018m1)
	drop periodo_renta
	gen year = year(dofm(date))
	duplicates drop benef_enciptado year, force
	keep benef_enciptado year
	tempfile e_`i'
	save `e_`i''
	local ++i
}
use `e_1'
forvalues y = 2/1000{
	append using `e_`y''
}
duplicates drop benef_enciptado year, force
