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
preserve 
keep benef_enciptado id_all
duplicates drop benef_enciptado, force
save "$usedata/sample_id.dta", replace
restore
bysort benef_enciptado: gen n = _n
preserve 
foreach n in 0 1{
	gen sex_`n' = sex if id_all == `n'
	gen foreign_`n' = foreign if id_all == `n'
}
graph bar (mean) sex_0 sex_1, asyvars bargap(10) blabel(bar, format(%9.2f)) ytitle("Percent") legend(label(1 "Males - id_all = 0") label(2 "Males - id_all = 1")) ylabel(0(0.2)1)
graph export "$graphs/Gender.png", replace
graph bar (mean) foreign_0 foreign_1, asyvars bargap(10) blabel(bar, format(%9.2f)) ytitle("Percent") legend(label(1 "Foreign - id_all = 0") label(2 "Foreign - id_all = 1") ) ylabel(0(0.2)1)
graph export "$graphs/Foreign.png", replace
restore
preserve 
encode tramo, gen(tram)
bysort benef_enciptado: egen maxtra = max(tram)
contract maxtra id_all, nomiss
bys id_all (maxtra) : replace _freq = sum(_freq)
bys id_all (maxtra) : replace _freq = _freq/_freq[_N]*100

twoway bar _freq id_all if maxtra == 4 , barw(.5) || ///
       bar _freq id_all if maxtra == 3 , barw(.5) || ///
       bar _freq id_all if maxtra == 2 , barw(.5) || ///
       bar _freq id_all if maxtra == 1 , barw(.5)    ///
       legend(order(1 "D"                    ///
                    2 "C"                         ///
                    3 "B"                      ///
                    4 "A"))                       ///
       ytitle("Percent")    xtitle("Sample")                           ///
       xlab(0/1, val)
	   graph export "$graphs/Tramo.png", replace
restore
distinct cod_comuna_pernat if id_all == 1
distinct cod_comuna_pernat if id_all == 0

preserve
replace tramo_renta = subinstr(tramo_renta, " a ", "-", .)
replace tramo_renta = subinstr(tramo_renta, " de ", "-", .)
* 2. Quitar puntos de miles
replace tramo_renta = subinstr(tramo_renta, ".", "", .)

* 3. Separar en dos variables: lower y upper
split tramo_renta, parse("-") destring

rename tramo_renta1 lower
rename tramo_renta2 upper
destring lower, replace force
* 4. Calcular la mediana
gen median = round((lower + upper)/2)
replace median = upper if lower == . & upper != .
foreach n in 0 1{
	bysort benef_enciptado: egen maxmedian_`n' = max(median) if id_all == `n'
	bysort benef_enciptado: egen maxdensidad_`n' = max(densidad) if id_all == `n'
	gen median_`n' = median if id_all == `n'
	gen densidad_`n' = densidad if id_all == `n'
}
estpost summarize maxmedian_0 maxmedian_1 maxdensidad_0 maxdensidad_1 if n == 1, d
esttab using "$graphs/med-den.tex", ///
    cells("mean(fmt(2)) sd(fmt(2)) p25 p50 p75") ///
    nonumber nomtitle label replace booktabs noobs
graph bar (mean) median_0 median_1, over(year) asyvars bargap(10) ytitle("Mean income") legend(label(1 "Income - id_all = 0") label(2 "Income - id_all = 1"))
graph export "$graphs/Income.png", replace
graph bar (mean) densidad_0 densidad_1 if year < 2022, over(year) asyvars bargap(10) ytitle("Mean months") legend(label(1 "Months worked - id_all = 0") label(2 "Months worked - id_all = 1"))
graph export "$graphs/Months.png", replace
restore
************************************************
*       2. Employer                *
************************************************
use "$usedata/Employer.dta", clear
gen year = year(dofm(date))
merge n:1 benef_enciptado using "$usedata/sample_id.dta", nogenerate keep(3)
merge n:1 codigo_empleador year using "$usedata/firm_size.dta", nogenerate keep(3)

bysort benef_enciptado year: egen nmon = max(_n)
replace nmon = nmon == 12
preserve
bysort benef_enciptado year: egen firm = max(fsize)
replace firm = firm == 3
duplicates drop benef_enciptado year, force
foreach n in 0 1{
	gen nmon_`n' = nmon if id_all == `n'
	gen firm_`n' = firm if id_all == `n'
}
graph bar (mean) nmon_0 nmon_1, over(year) ytitle("% of workers with full employment") ///
    blabel(bar, format(%4.2f)) ylabel(0(0.2)1) bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
	graph export "$graphs/fullyear_emp.png", replace
graph bar (mean) firm_0 firm_1, over(year) ytitle("% of workers in large firms") ///
    blabel(bar, format(%4.2f)) ylabel(0(0.2)1) bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
	graph export "$graphs/fullyear_firm_large.png", replace
restore
preserve
gen month = month(dofm(date))
bysort benef_enciptado year: egen minx1 = min(monto_renta_imponible_pesos) if month <= 6
bysort benef_enciptado year: egen maxx1 = max(monto_renta_imponible_pesos) if month <= 6
bysort benef_enciptado year: egen minx2 = min(monto_renta_imponible_pesos) if month > 6
bysort benef_enciptado year: egen maxx2 = max(monto_renta_imponible_pesos) if month > 6
gen same_x = (maxx1-minx1)/minx1 <0.2 | (maxx2-minx2)/minx2 <0.2
replace same_x = 0 if nmon != 1
bysort benef_enciptado year: egen same = min(same_x)
bysort benef_enciptado year: egen jobs = max(n)
replace jobs = jobs > 1
duplicates drop benef_enciptado year, force
foreach n in 0 1{
	gen same_`n' = same if id_all == `n'
	gen jobs_`n' = jobs if id_all == `n'
}
graph bar (mean) same_0 same_1 if nmon == 1, over(year) ytitle("% of workers with constant salary") ///
    blabel(bar, format(%4.2f)) ylabel(0(0.2)1) bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
graph export "$graphs/fullyear_salary.png", replace
graph bar (mean) jobs_0 jobs_1 , over(year) ytitle("% of workers with more than one job") ///
    blabel(bar, format(%4.2f)) ylabel(0(0.2)1) bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
graph export "$graphs/fullyear_jobs.png", replace
restore
preserve
foreach n in 0 1{
	gen monto_renta_imponible_pesos_`n' = monto_renta_imponible_pesos if id_all == `n'
}
graph bar (mean) monto_renta_imponible_pesos_0 monto_renta_imponible_pesos_1, over(year) ytitle("Mean salary") ///
     bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1")) ylabel(0(300000)900000)
	graph export "$graphs/fullyear_mean_salary.png", replace
restore
************************************************
*       3. Sick leaves                *
************************************************
use "$usedata/Sick.dta", clear
merge n:1 benef_enciptado using "$usedata/sample_id.dta", nogenerate keep(3)
gen year = year(date)
gen suma = 1
preserve
duplicates drop benef_enciptado year, force
bysort id_all year: egen total = total(suma)
gen total_1 = total/81764 if id_all == 1
gen total_0 = total/38974 if id_all == 0
graph bar (mean) total_0 total_1 , over(year) ytitle("% of workers with sick leaves") ///
    blabel(bar, format(%4.2f)) ylabel(0(0.2)1) bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
graph export "$graphs/percent-sl.png", replace
restore
preserve
bysort benef_enciptado year: egen lic = total(suma)
duplicates drop benef_enciptado year, force
histogram lic if id_all == 1, freq ylabel(,format(%9.0f)) discrete
graph export "$graphs/hist_1.png", replace
histogram lic if id_all == 0, freq ylabel(,format(%9.0f)) discrete
graph export "$graphs/hist_o.png", replace
restore
preserve 
bysort id_all year: egen mean = mean(dias_otorgados)
duplicates drop benef_enciptado year, force

foreach n in 0 1{
	gen mean_`n' = mean if id_all == `n'
}
graph bar (mean) mean_0 mean_1 , over(year) ytitle("Mean sick leave (days)") ///
     bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
graph export "$graphs/days-sl.png", replace
restore
preserve 
bysort benef_enciptado year: egen mean = total(dias_otorgados)
duplicates drop benef_enciptado year, force

foreach n in 0 1{
	gen mean_`n' = mean if id_all == `n'
}
graph bar (mean) mean_0 mean_1 , over(year) ytitle("Total sick leave (days)") ///
     bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
graph export "$graphs/totdays-sl.png", replace
restore
gen month_num = month(date)
gen month_var = mofd(date)
format month_var %tm
bysort month_var: egen total = total(suma)

twoway line total month_var, ytitle("Total sick leave") xtitle("Date")
graph export "$graphs/tm-sl.png", replace
twoway line total month_var, xline(698 710 722 734 746 758 770 782) ytitle("Total sick leave") xtitle("Date")
graph export "$graphs/tmm-sl.png", replace
bysort month_num: egen total2 = total(suma)
twoway line total2 month_num
graph export "$graphs/mnum-sl.png", replace
************************************************
*       4. Pollution                  *
************************************************
use "$usedata/pollution.dta", clear
foreach n in MP10 MP25 NOX O3{
	bysort cod_comuna: egen total_`n'= count(mean`n') 
}
tab total_MP25 if total_MP25 != 0 & date == td(01jan2018)
collapse (sum) mean*, by(cod_comuna)
replace meanMP10 = 1 if meanMP10 >0
replace meanMP25 = 1 if meanMP25 >0
replace meanNOX = 1 if meanNOX >0
replace meanO3 = 1 if meanO3 >0
tab meanMP10
tab meanMP25
tab meanNOX
tab meanO3
tempfile pol
save `pol'
use "$usedata/Population.dta", clear
sort benef_enciptado year
bysort benef_enciptado: replace foreign = foreign[_n-1] if year == 2021
bysort benef_enciptado: egen max = max(_n)
gen id_all = max == 7
rename cod_comuna_pernat cod_comuna
merge n:1 cod_comuna using `pol'
duplicates drop benef_enciptado, force
replace meanMP10 = 0 if meanMP10 ==.
replace meanMP25 = 0 if meanMP25 ==.
replace meanNOX = 0 if meanNOX ==.
replace meanO3 = 0 if meanO3 ==.
tab meanMP10 id_all
tab meanMP25 id_all
tab meanNOX id_all
tab meanO3 id_all

import excel "$rawdata\pollution\entidades_centros.xlsx", firstrow clear
destring Pers, replace
gen suma = Pers if Estación != "."
collapse (sum) Pers suma, by(cod_comuna)
gen share = suma/Pers
keep if share != 0
sort share
gen n = _n
twoway area share n, xlabel(0(10)134) ytitle("Share of population with pollution data") xtitle("Municipality")
graph export "$graphs/share_pollution.png", replace

************************************************
*       5. Climate               *
************************************************
use "$usedata/climate_tmin.dta", clear
gen month = month(date)

xtreg 
gen year = year(date)
collapse (mean) value, by(cod_comuna year)
drop if cod_comuna == .
reshape wide value, i(year) j(cod_comuna)
twoway line value* year, legend(subtitle("Life expectancy") order(1 "White males" 2 "Black males"))







