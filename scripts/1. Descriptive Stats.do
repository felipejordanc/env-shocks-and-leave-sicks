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
*       1. Population                  *
************************************************
use "$usedata/Population.dta", clear
sort benef_enciptado year
bysort benef_enciptado: replace foreign = foreign[_n-1] if year == 2021
bysort benef_enciptado: egen max = max(_n)
gen id_all = max == 7
bysort benef_enciptado: gen n = _n


************************************************
*       2. Employer                *
************************************************
use "$usedata/Employer.dta", clear
gen year = year(dofm(date))
bysort benef_enciptado year: egen nmon = max(_n)
replace nmon = nmon == 12
preserve
duplicates drop benef_enciptado year, force
graph bar (mean) nmon, over(year) ytitle("% of workers with full employment") ///
    blabel(bar, format(%4.2f)) ylabel(0(0.2)1) bargap(20) legend(off)
	graph export "$graphs/fullyear_emp.png"
restore
gen month = month(dofm(date))
bysort benef_enciptado year: egen minx1 = min(monto_renta_imponible_pesos) if month <= 6
bysort benef_enciptado year: egen maxx1 = max(monto_renta_imponible_pesos) if month <= 6
bysort benef_enciptado year: egen minx2 = min(monto_renta_imponible_pesos) if month > 6
bysort benef_enciptado year: egen maxx2 = max(monto_renta_imponible_pesos) if month > 6
gen same_x = (minx1 == maxx1) & (minx2 == maxx2)
replace same_x = 0 if nmon != 1

************************************************
*       3. Sick leaves                *
************************************************
use "$usedata/Sick.dta", clear



************************************************
*       4. Pollution                  *
************************************************
use "$usedata/pollution.dta", clear


************************************************
*       5. Climate               *
************************************************
use "$usedata/climate_pr.dta", clear










