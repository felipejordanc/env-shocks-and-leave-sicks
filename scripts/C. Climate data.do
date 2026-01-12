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
*       1. Climate raw database                *
************************************************
foreach v in tmax tmin pr{
	import delimited "E:\Sick leave\microdatos_manzana\Centroide/`v'_rural.csv", varnames(1) clear
	drop if entity == 16180 |entity == 16437 |entity == 16578 |entity == 16692 |entity == 16693 |entity == 16694 |entity == 16695 |entity == 16696 |entity == 16697 |entity == 16698 |entity == 16699 |entity == 16700 |entity == 16701 |entity == 16702 |entity == 16703 |entity == 27600 |entity == 28120 |entity == 28197 
	gen shp = "ru"
	tempfile rur
	save `rur'
	import delimited "E:\Sick leave\microdatos_manzana\Centroide/`v'_manzana.csv", varnames(1) clear
	drop if entity >= 91858 & entity <= 91870
	drop if entity >= 92890 & entity <= 92914
	drop if entity == 91928 | entity == 92103 | entity == 92492 | entity == 92505
	gen shp = "ma"
	append using `rur'
	rename date date2
	gen date = daily(date2, "YMD")
	format date %td
	preserve
	duplicates drop entity shp, force
	collapse (sum) total_pers, by(cod_comuna)
	rename total_pers total_pop
	tempfile com
	save `com'
	restore
	merge m:1 cod_comuna using `com', nogenerate
	gen pob_w = total_pers/total_pop
	replace value = value*pob_w
	collapse (sum) value, by(date cod_comuna)
	save "$usedata/climate_`v'.dta", replace
	clear all
}


************************************************
*      2. Climate monthly database             *
************************************************
set maxvar 32000
use "$usedata/climate_tmax.dta", clear
rename date date_td
gen date = mofd(date_td)
format date %tm
collapse (max) value, by(cod_comuna date)
replace value = round(value)
save "$usedata/tmax_reg.dta", replace

************************************************
*      3. Climate daily database             *
************************************************

use "$usedata/climate_tmax.dta", clear
rename value tmax
merge 1:1 cod_comuna date using "$usedata/climate_tmin.dta"
rename value tmin
collapse (max) tmax (min) tmin, by(cod_comuna date)
replace tmax = round(tmax)
replace tmin = round(tmin)

save "$usedata/tmax_w.dta", replace
************************************************
*      4. Climate bin database             *
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
merge n:1 cod_comuna date using "$usedata/tmax_w.dta", keep(1 3) nogenerate
gen week = wofd(date)
format week %tw
compress
preserve
gen tmin_dum_0 = tmin <0
gen tmin_dum_20 = tmin >19 & tmin != .
gen tmin_mean = tmin
forvalues i=0/9{
	gen tmin_dum_`=`i'*2'_`=`i'*2+1' = 0
	replace tmin_dum_`=`i'*2'_`=`i'*2+1' = 1 if tmin == `i'*2 | tmin == `i'*2 + 1
}
gen suma = 1
compress
collapse (sum) tmin_dum* suma (min) tmin (mean) tmin_mean, by(benef_enciptado week)
forvalues i=0/9{
	replace tmin_dum_`=`i'*2'_`=`i'*2+1' = tmin_dum_`=`i'*2'_`=`i'*2+1'/suma 
}
drop tmin_dum_8_9

replace tmin_dum_0 = tmin_dum_0/ suma
replace tmin_dum_20 = tmin_dum_20/suma
drop suma
compress
rename week date
save "$usedata/tmin_dum.dta", replace
restore
gen tmax_dum_9 = tmax <10
gen tmax_dum_38 = tmax >37 & tmax != .
gen tmax_mean = tmax

forvalues i=5/18{
	gen tmax_dum_`=`i'*2'_`=`i'*2+1' = 0
	replace tmax_dum_`=`i'*2'_`=`i'*2+1' = 1 if tmax == `i'*2 | tmax == `i'*2 + 1
}
gen suma = 1
compress
collapse (sum) tmax_dum* suma (max) tmax (mean) tmax_mean, by(benef_enciptado week)
forvalues i=5/18{
	replace tmax_dum_`=`i'*2'_`=`i'*2+1' = tmax_dum_`=`i'*2'_`=`i'*2+1'/suma
}
drop tmax_dum_20_21
replace tmax_dum_9 = tmax_dum_9/ suma
replace tmax_dum_38 = tmax_dum_38/suma
compress
rename week date
save "$usedata/tmax_dum.dta", replace
















