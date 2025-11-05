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
preserve
destring edad_tramo, replace force
bysort benef_enciptado: egen edad_2021 = total(edad_tramo)
gen age = edad_2021 - (2021 - year) if edad_2021 != 0
foreach n in 0 1{
	gen age_`n' = age if id_all == `n'
}
estpost summarize age_0 age_1 if n == 1, d
esttab using "$graphs/age_pop.tex", ///
    cells("mean(fmt(2)) sd(fmt(2)) p25 p50 p75") ///
    nonumber nomtitle label replace booktabs noobs
************************************************
*       2. Employer                *
************************************************
use "$usedata/Employer.dta", clear
gen year = year(dofm(date))
merge n:1 benef_enciptado using "$usedata/sample_id.dta", nogenerate keep(3)
merge n:1 codigo_empleador year using "$usedata/firm_size.dta", nogenerate keep(3)
merge n:1 date using "$usedata/UF.dta", nogenerate keep(3)
replace monto_renta_imponible_pesos = monto_renta_imponible_pesos/UF
bysort benef_enciptado year: egen nmon = max(_n)
preserve
foreach n in 0 1{
	gen monto_`n' = monto_renta_imponible_pesos if id_all == `n'
	gen densidad_`n' = nmon if id_all == `n'
}
estpost summarize monto_0 monto_1 densidad_0 densidad_1 if n == 1, d
esttab using "$graphs/med-den_emp.tex", ///
    cells("mean(fmt(2)) sd(fmt(2)) p25 p50 p75") ///
    nonumber nomtitle label replace booktabs noobs
restore
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
     bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1")) 
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
bysort benef_enciptado year: egen total =total(suma)
duplicates drop benef_enciptado year, force

foreach n in 0 1{
	gen mean_`n' = mean if id_all == `n'
	gen total_`n' = total if id_all == `n'
	}
graph bar (mean) mean_0 mean_1 , over(year) ytitle("Mean sick leave (days)") ///
     bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
graph export "$graphs/days-sl.png", replace
graph bar (mean) total_0 total_1 , over(year) ytitle("Mean sick leave") ///
     bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
graph export "$graphs/mean-sl.png", replace
restore
preserve 
merge n:1 benef_enciptado using "$usedata/sample_id.dta", nogenerate 
fillin benef_enciptado year
bysort benef_enciptado: replace id_all = id_all[_n-1] if id_all == .
bysort benef_enciptado: replace id_all = id_all[_N] if id_all == .
replace suma = 0 if suma == .
bysort benef_enciptado year: egen total =total(suma)
duplicates drop benef_enciptado year, force

foreach n in 0 1{
	gen total_`n' = total if id_all == `n'
}
graph bar (mean) total_0 total_1 , over(year) ytitle("Mean sick leave") ///
     bargap(20) legend(label(1 "id_all = 0") label(2 "id_all = 1"))
graph export "$graphs/mean-sl_all.png", replace
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
collapse (sum) suma, by(month_var)
tsset month_var
tssmooth ma ma_var = suma, window(3)
twoway line suma month_var, ytitle("Total sick leave") xtitle("Date")
graph export "$graphs/tm-sl.png", replace
twoway line ma_var month_var, ytitle("Total sick leave") xtitle("Date")
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

use "$usedata/pollution.dta", clear
gen suma = 1
foreach m in MP10 MP25 NOX O3{
	gen missing`m' = mean`m' == .
}
collapse (sum) suma missing*, by(cod_comuna)
foreach m in MP10 MP25 NOX O3{
	replace missing`m' = . if missing`m' == 2557
}
foreach m in MP10 MP25 NOX O3{
	preserve
	sort missing`m'
	drop if missing`m' == .
	gen n = _n
	summ missing`m'
	local n = r(N)
	twoway area missing`m' n, ytitle("Number of missing days - `m'") xtitle("Municipality") xlabel(0(5)`n')
	graph export "$graphs/missing_`m'_pollution.png", replace
	restore
}
import delimited "$rawdata\pollution\Data_Pollution_cleaned.csv", varnames(1) clear
gen date = daily(fechayymmdd, "YMD")
format date %td
drop fechayymmdd
keep if medida == "Material_particulado_MP_10" | medida == "Material_particulado_MP_25" | medida == "Ozono-" | medida == "xidos_de_nitrgeno"
replace medida = "MP10" if medida == "Material_particulado_MP_10"
replace medida = "MP25" if medida == "Material_particulado_MP_25"
replace medida = "NOX" if medida == "xidos_de_nitrgeno"
replace medida = "O3"  if medida == "Ozono-"
rename centro Estacion
gen suma = 1
drop if min_val==0 & max_val==0 & mean_val==0
drop min_val max_val mean_val
save "$usedata/centers_nomiss.dta", replace

************************************************
*       5. Climate               *
************************************************
*Creating dbs
foreach y in 2018 2019 2020{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	merge 1:1 benef_enciptado using "$usedata/benef_sample.dta", nogenerate keep(3)
	gen sex = sexo_cor == "HOMBRE"
	gen carga = titular_carga == "CARGA"
	gen foreign = nacionalidad_cor == "EXTRANJERA"
	keep benef_enciptado edad_tramo cod_comuna_pernat tramo desc_tipo_asegurado densidad tramo_renta sex carga foreign
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}


foreach y in 2022 2023 2024{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	rename código_beneficiario benef_enciptado
	merge 1:1 benef_enciptado using "$usedata/benef_sample.dta", nogenerate keep(3)
	gen sex = sexo == "Hombre"
	gen carga = titular_carga == "Carga"
	gen foreign = nacionalidad == "Extranjera"
	merge n:1 comuna_beneficiario using "C:\Users\black\Dropbox\GSL\Bases intermedias\codigo_comunas.dta", update nogenerate keep(3)
	rename tramo_fonasa tramo
	rename tipo_asegurado desc_tipo_asegurado
	keep benef_enciptado edad_tramo cod_comuna_pernat tramo desc_tipo_asegurado tramo_renta sex carga foreign
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}
import delimited "$rawdata/population/Benef_FNS_2021\Benef_FNS_2021.txt", clear varnames(1)
rename id_asegurado benef_enciptado
merge 1:1 benef_enciptado using "$usedata/benef_sample.dta", nogenerate keep(3)
merge n:1 comuna using "C:\Users\black\Dropbox\codigo_comunas.dta", update nogenerate keep(3)
rename grupo tramo
rename tipo_trabajador desc_tipo_asegurado
rename tramo_ingreso tramo_renta
gen sex = sexo == "HOMBRE"
gen carga = titular_carga == "CARGA"
gen year = 2021
rename edad edad_tramo
tostring edad_tramo, replace
drop año fec fecha_nacimiento sexo titular_carga id_titular comuna mto_cotiz_mes comuna_beneficiario
tempfile b_2021
save `b_2021'

use `b_2018'
foreach y in 2019 2020 2021 2022 2023 2024{
	append using `b_`y''
}
preserve
keep benef_enciptado cod_comuna_pernat year
rename cod_comuna_pernat cod_comuna
save "$usedata/id_com.dta"
restore
gen suma = 1
collapse (sum) suma, by(cod_comuna_pernat year)
rename cod_comuna_pernat cod_comuna
save "$usedata/cot_pop.dta"

import delimited "$rawdata/sick leaves\T8314.csv", clear varnames(1)
gen date = daily(fecha_emision, "DMY")
format date %td
gen date2 = daily(fecha_desde, "DMY")
format date2 %td
gen delta = (date - date2)>=0
label define deltal 1 "Reactive" 0 "Proactive"
label values delta deltal
gen year = year(date)
rename encripbi_rut_traba benef_enciptado
merge n:1 benef_enciptado year using "$usedata/id_com.dta", nogenerate keep(3)
gen suma= 1
collapse (sum) suma, by(cod_comuna date year delta)
rename suma sick
merge n:1 cod_comuna year using "$usedata/cot_pop.dta", nogenerate keep(3)
gen y = (sick/suma)*1000
xtset cod_comuna date
reg y i.date i.cod_comuna
predict resid, residuals
tssmooth ma ma_sl = sick, window(0 1 7)
gen y2 = (ma_sl/suma)*1000
reg y2 i.date i.cod_comuna
predict resid2, residuals
keep date cod_comuna resid resid2
save "$usedata/resid_sl.dta", replace


*Residuals


use "$usedata/pollution.dta", clear
foreach v in meanMP25 meanNOX meanO3 meanMP10{
	preserve
	reg `v' i.date i.cod_comuna
	predict resid, residuals
	keep date cod_comuna resid
	rename resid resid_`v'
	merge 1:1 cod_comuna date using "$usedata/resid_sl.dta", nogenerate keep(3)
	scatter resid resid_`v', ytitle("SL residuals") xtitle("`v' residuals")  color(cyan%10) msize(vtiny)
	graph export "$graphs/resid_`v'.png", replace
	scatter resid2 resid_`v', ytitle("SL residuals - MA") xtitle("`v' residuals")  color(cyan%10) msize(vtiny)
	graph export "$graphs/resid_`v'_ma.png", replace
	restore
}

use "$usedata/climate_tmin.dta", clear
reg value i.date i.cod_comuna
predict resid, residuals
keep date cod_comuna resid
rename resid resid_tmin
merge 1:1 cod_comuna date using "$usedata/resid_sl.dta", nogenerate keep(3)
scatter resid resid_tmin, ytitle("SL residuals") xtitle("Tmin residuals")  color(cyan%10) msize(vtiny)
graph export "$graphs/resid_tmin.png", replace
scatter resid2 resid_tmin, ytitle("SL residuals - MA") xtitle("Tmin residuals")  color(cyan%10) msize(vtiny)
graph export "$graphs/resid_tmin_ma.png", replace

use "$usedata/climate_tmax.dta", clear
reg value i.date i.cod_comuna
predict resid, residuals
keep date cod_comuna resid
rename resid resid_tmin
merge 1:1 cod_comuna date using "$usedata/resid_sl.dta", nogenerate keep(3)
scatter resid resid_tmin, ytitle("SL residuals") xtitle("Tmax residuals")  color(cyan%10) msize(vtiny)
graph export "$graphs/resid_tmax.png", replace
scatter resid2 resid_tmin, ytitle("SL residuals - MA") xtitle("Tmax residuals")  color(cyan%10) msize(vtiny)
graph export "$graphs/resid_tmax_ma.png", replace

use "$usedata/climate_pr.dta", clear
reg value i.date i.cod_comuna
predict resid, residuals
keep date cod_comuna resid
rename resid resid_tmin
merge 1:1 cod_comuna date using "$usedata/resid_sl.dta", nogenerate keep(3)
scatter resid resid_tmin , ytitle("SL residuals") xtitle("Precipitation residuals")  color(cyan%10) msize(vtiny)
graph export "$graphs/resid_pr.png", replace
merge 1:1 cod_comuna date using "$usedata/resid_sl.dta", nogenerate keep(3)
scatter resid2 resid_tmin, ytitle("SL residuals - MA") xtitle("Precipitation residuals")  color(cyan%10) msize(vtiny)
graph export "$graphs/resid_pr_ma.png", replace
